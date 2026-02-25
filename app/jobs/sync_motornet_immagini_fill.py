import os
import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from sqlalchemy import text

from app.database import DBSession
from app.external.motornet import motornet_get


# ============================================================
# ENDPOINT
# ============================================================

NUOVO_IMMAGINI_URL = "https://webservice.motornet.it/api/v3_0/rest/public/nuovo/auto/immagini"


# ============================================================
# CONFIG (manual-run friendly)
# ============================================================

DEFAULT_CONCURRENCY = int(os.getenv("MNET_IMG_CONCURRENCY", "6"))
DEFAULT_BATCH_SIZE = int(os.getenv("MNET_IMG_BATCH_SIZE", "60"))  # quanti codici per chunk
LIMIT_CODES = int(os.getenv("MNET_IMG_LIMIT_CODES", "0"))  # 0 = tutti
ONLY_CODES_WITH_LT = int(os.getenv("MNET_IMG_ONLY_LT", "0"))  # 0 = tutti, altrimenti filtra codici con count < N


# ============================================================
# SQL
# ============================================================

SQL_GET_CODES_ALL = """
SELECT d.codice_motornet_uni
FROM public.mnet_dettagli d
WHERE d.codice_motornet_uni IS NOT NULL
ORDER BY d.codice_motornet_uni;
"""

SQL_GET_CODES_ONLY_LT = """
WITH cnt AS (
  SELECT codice_motornet_uni, COUNT(*) AS n
  FROM public.mnet_immagini
  GROUP BY codice_motornet_uni
)
SELECT d.codice_motornet_uni
FROM public.mnet_dettagli d
LEFT JOIN cnt c ON c.codice_motornet_uni = d.codice_motornet_uni
WHERE d.codice_motornet_uni IS NOT NULL
  AND COALESCE(c.n, 0) < :max_n
ORDER BY d.codice_motornet_uni;
"""

SQL_COUNT_FOR_CODE = """
SELECT COUNT(*) FROM public.mnet_immagini WHERE codice_motornet_uni = :codice;
"""

SQL_UPSERT_IMAGE = """
INSERT INTO public.mnet_immagini (
  codice_motornet_uni,
  url,
  codice_fotografia,
  codice_visuale,
  descrizione_visuale,
  risoluzione,
  created_at,
  updated_at
)
VALUES (
  CAST(:codice AS varchar),
  CAST(:url AS text),
  CAST(:codice_fotografia AS varchar),
  CAST(:codice_visuale AS varchar),
  CAST(:descrizione_visuale AS text),
  CAST(:risoluzione AS varchar),
  now(),
  now()
)
ON CONFLICT (codice_motornet_uni, url)
DO UPDATE SET
  codice_fotografia = COALESCE(EXCLUDED.codice_fotografia, public.mnet_immagini.codice_fotografia),
  codice_visuale = COALESCE(EXCLUDED.codice_visuale, public.mnet_immagini.codice_visuale),
  descrizione_visuale = COALESCE(EXCLUDED.descrizione_visuale, public.mnet_immagini.descrizione_visuale),
  risoluzione = COALESCE(EXCLUDED.risoluzione, public.mnet_immagini.risoluzione),
  updated_at = now();
"""


# ============================================================
# LOGIC (frozen)
# - grouping: (codiceVisuale, codiceFotografia)
# - choose 1 per group: H > M > L
# ============================================================

_RES_PRIORITY = {"H": 3, "M": 2, "L": 1}


def _norm_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _pick_best(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    # H > M > L; tie-break stable by url
    def key(it: Dict[str, Any]) -> Tuple[int, str]:
        r = _norm_str(it.get("risoluzione"))
        score = _RES_PRIORITY.get(r or "", 0)
        url = _norm_str(it.get("url")) or ""
        return (score, url)

    return sorted(items, key=key, reverse=True)[0]


def select_images(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = payload.get("immagini") or []
    if not isinstance(raw, list):
        return []

    # group by (codiceVisuale, codiceFotografia)
    groups: Dict[Tuple[Optional[str], Optional[str]], List[Dict[str, Any]]] = defaultdict(list)

    for it in raw:
        if not isinstance(it, dict):
            continue

        url = _norm_str(it.get("url"))
        if not url:
            continue

        cv = _norm_str(it.get("codiceVisuale"))
        cf = _norm_str(it.get("codiceFotografia"))

        # grouping A) = (codiceVisuale, codiceFotografia)
        # fallback safe: if cf is None, group by (cv, url) to avoid collapsing unrelated images
        if cf is None:
            groups[(cv, url)].append(it)
        else:
            groups[(cv, cf)].append(it)

    selected: List[Dict[str, Any]] = []
    for _k, items in groups.items():
        best = _pick_best(items)
        url = _norm_str(best.get("url"))
        if not url:
            continue
        selected.append(best)

    # final dedup by url (safety)
    seen = set()
    out = []
    for it in selected:
        url = _norm_str(it.get("url"))
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(it)

    return out


# ============================================================
# PIPELINE
# ============================================================

async def fetch_one(code: str, sem: asyncio.Semaphore) -> Tuple[str, Optional[Dict[str, Any]], Optional[str]]:
    async with sem:
        try:
            data = await motornet_get(f"{NUOVO_IMMAGINI_URL}?codice_motornet_uni={code}")
            return (code, data, None)
        except Exception as e:
            return (code, None, str(e))


def load_codes() -> List[str]:
    with DBSession() as db:
        if ONLY_CODES_WITH_LT and ONLY_CODES_WITH_LT > 0:
            rows = db.execute(text(SQL_GET_CODES_ONLY_LT), {"max_n": ONLY_CODES_WITH_LT}).fetchall()
        else:
            rows = db.execute(text(SQL_GET_CODES_ALL)).fetchall()

    codes = [r[0] for r in rows]
    if LIMIT_CODES and LIMIT_CODES > 0:
        codes = codes[:LIMIT_CODES]
    return codes


def db_count_for_code(code: str) -> int:
    with DBSession() as db:
        return int(db.execute(text(SQL_COUNT_FOR_CODE), {"codice": code}).scalar() or 0)


def upsert_selected(code: str, selected: List[Dict[str, Any]]) -> int:
    if not selected:
        return 0

    params_list = []
    for it in selected:
        params_list.append(
            {
                "codice": code,
                "url": _norm_str(it.get("url")),
                "codice_fotografia": _norm_str(it.get("codiceFotografia")),
                "codice_visuale": _norm_str(it.get("codiceVisuale")),
                "descrizione_visuale": _norm_str(it.get("descrizioneVisuale")),
                "risoluzione": _norm_str(it.get("risoluzione")),
            }
        )

    with DBSession() as db:
        db.execute(text(SQL_UPSERT_IMAGE), params_list)

    return len(params_list)


def chunked(seq: List[str], n: int) -> List[List[str]]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


def run() -> None:
    logging.info(
        "[NUOVO][IMMAGINI_FILL] START (only_lt=%s, limit=%s, concurrency=%d, batch=%d)",
        ONLY_CODES_WITH_LT if ONLY_CODES_WITH_LT else "ALL",
        LIMIT_CODES if LIMIT_CODES else "NONE",
        DEFAULT_CONCURRENCY,
        DEFAULT_BATCH_SIZE,
    )

    codes = load_codes()
    total = len(codes)

    if total == 0:
        logging.info("[NUOVO][IMMAGINI_FILL] NOTHING TO DO")
        return

    inserted_total = 0
    failed_total = 0
    processed = 0

    async def _run_batch(batch: List[str]) -> List[Tuple[str, Optional[Dict[str, Any]], Optional[str]]]:
        sem = asyncio.Semaphore(DEFAULT_CONCURRENCY)
        tasks = [fetch_one(c, sem) for c in batch]
        return await asyncio.gather(*tasks)

    for batch in chunked(codes, DEFAULT_BATCH_SIZE):
        results = asyncio.run(_run_batch(batch))

        for code, payload, err in results:
            processed += 1

            if err:
                failed_total += 1
                logging.error("[NUOVO][IMMAGINI_FILL] %s FAILED (motornet=%s)", code, err)
                continue

            selected = select_images(payload or {})
            before = db_count_for_code(code)

            upserted = upsert_selected(code, selected)
            after = db_count_for_code(code)

            # Nota: upserted = righe inviate al DB, non “nuove”.
            # Il delta reale è (after - before).
            delta = after - before
            inserted_total += max(delta, 0)

            logging.info(
                "[NUOVO][IMMAGINI_FILL] %s raw=%d selected=%d before=%d after=%d delta=%d",
                code,
                len((payload or {}).get("immagini") or []),
                len(selected),
                before,
                after,
                delta,
            )

    logging.info(
        "[NUOVO][IMMAGINI_FILL] DONE (processed=%d/%d, inserted_new=%d, failed=%d)",
        processed,
        total,
        inserted_total,
        failed_total,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    run()