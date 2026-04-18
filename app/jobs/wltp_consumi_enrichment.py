"""
WLTP consumi / CO2 enrichment per mnet_dettagli_usato (e mnet_vcom_dettagli).

Motornet espone i consumi WLTP su endpoint separato rispetto a /dettaglio:
    GET /api/v2_0/rest/public/usato/auto/dettaglio/wltp?codice_motornet={codice}
    GET /api/v3_0/rest/public/usato/vcom/dettaglio/wltp?codice_motornet_uni={codice}

L'endpoint /dettaglio standard (usato dal worker che popola mnet_dettagli_usato)
ha i campi consumo_* NEDC che per auto solo-WLTP (post-2018) sono NULL, mentre
la risposta /dettaglio/wltp contiene consumoCombinato e co2Combinato valorizzati.

Questo worker fa un secondo passo: per ogni codice con consumi/CO2 NULL in
mnet_dettagli_usato, chiama /dettaglio/wltp ed esegue UPDATE sui campi NULL.
Non sovrascrive mai valori esistenti, non tocca altri campi.
"""

import asyncio
import logging
from sqlalchemy import text

from app.database import DBSession
from app.external.motornet import motornet_get
from app.jobs.wltp_enrichment import is_vcom, build_wltp_url

BATCH_SIZE = 100
MAX_CONCURRENCY = 10  # richieste Motornet parallele

logger = logging.getLogger(__name__)


def _to_float(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return float(s.replace(",", "."))
        except ValueError:
            return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _pick_wltp_record(records: list[dict]) -> dict | None:
    """Sceglie il record WLTP più recente con valori consumo/CO2 valorizzati.

    Preferenza:
    1. Record senza dataFineValidita (= tuttora valido)
    2. Record con dataFineValidita più alta
    Tie-break: quello con più campi non-null tra consumoCombinato/co2Combinato.
    """
    if not records:
        return None

    def score(r: dict) -> tuple[int, str, int]:
        cc = _to_float(r.get("consumoCombinato"))
        co2 = _to_float(r.get("co2Combinato"))
        populated = (1 if cc is not None else 0) + (1 if co2 is not None else 0)
        end = r.get("dataFineValidita") or ""
        open_ended = 1 if not end else 0
        return (populated, end, open_ended)

    ranked = sorted(records, key=score, reverse=True)
    best = ranked[0]

    if _to_float(best.get("consumoCombinato")) is None and _to_float(best.get("co2Combinato")) is None:
        return None
    return best


async def _fetch_wltp_for_codes(codici: list[str]) -> dict[str, list[dict] | Exception]:
    """Chiama /dettaglio/wltp in parallelo con semaphore per limitare il fan-out."""
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    results: dict[str, list[dict] | Exception] = {}

    async def _one(codice: str) -> None:
        async with sem:
            try:
                data = await motornet_get(build_wltp_url(codice))
                results[codice] = data.get("wltp", []) or []
            except Exception as e:
                results[codice] = e

    await asyncio.gather(*(_one(c) for c in codici))
    return results


def _fetch_batch_codes(db) -> list[dict]:
    """Seleziona codici da arricchire. mnet_dettagli_usato prima, poi mnet_vcom_dettagli."""
    usato = db.execute(
        text("""
            SELECT codice_motornet_uni AS codice, 'AUTO' AS tipo
            FROM mnet_dettagli_usato
            WHERE (consumo_medio IS NULL OR emissioni_co2 IS NULL)
              AND codice_motornet_uni IS NOT NULL
            ORDER BY random()
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        """),
        {"limit": BATCH_SIZE},
    ).mappings().all()
    return [dict(r) for r in usato]


def _update_consumi(db, codice: str, tipo: str, cc: float | None, co2: float | None) -> bool:
    """UPDATE solo dove i campi sono ancora NULL. Ritorna True se qualcosa è stato scritto."""
    if cc is None and co2 is None:
        return False

    table = "mnet_dettagli_usato" if tipo == "AUTO" else "mnet_vcom_dettagli"

    res = db.execute(
        text(f"""
            UPDATE {table}
            SET consumo_medio = COALESCE(consumo_medio, :cc),
                emissioni_co2 = COALESCE(emissioni_co2, :co2)
            WHERE codice_motornet_uni = :codice
              AND (consumo_medio IS NULL OR emissioni_co2 IS NULL)
        """),
        {"codice": codice, "cc": cc, "co2": co2},
    )
    return res.rowcount > 0


def wltp_consumi_enrichment_worker() -> None:
    logger.info("[WLTP-CONSUMI] START")

    with DBSession() as db:
        rows = _fetch_batch_codes(db)

        if not rows:
            logger.info("[WLTP-CONSUMI] NOTHING TO DO")
            return

        codici = [r["codice"] for r in rows]
        fetched = asyncio.run(_fetch_wltp_for_codes(codici))

        updated = 0
        nd_count = 0
        err_count = 0

        for row in rows:
            codice = row["codice"]
            tipo = row["tipo"]
            result = fetched.get(codice)

            try:
                if isinstance(result, Exception):
                    msg = str(result)
                    if "PRECONDITION_FAILED" in msg or "412" in msg:
                        logger.info("[WLTP-CONSUMI] %s: nessun record WLTP (412)", codice)
                        nd_count += 1
                        continue
                    logger.warning("[WLTP-CONSUMI] %s FETCH FAIL: %s", codice, msg[:120])
                    err_count += 1
                    continue

                best = _pick_wltp_record(result)
                if not best:
                    logger.info("[WLTP-CONSUMI] %s: record WLTP senza valori utili", codice)
                    nd_count += 1
                    continue

                cc = _to_float(best.get("consumoCombinato"))
                co2 = _to_float(best.get("co2Combinato"))

                if _update_consumi(db, codice, tipo, cc, co2):
                    logger.info(
                        "[WLTP-CONSUMI] %s → consumo=%s co2=%s",
                        codice, cc, co2,
                    )
                    updated += 1
                else:
                    nd_count += 1

            except Exception:
                logger.exception("[WLTP-CONSUMI] %s PROCESS FAIL", codice)
                err_count += 1

        db.commit()

    logger.info(
        "[WLTP-CONSUMI] DONE updated=%d nd=%d err=%d total=%d",
        updated, nd_count, err_count, len(rows),
    )
