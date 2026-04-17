"""
Sync AutoScout24 Reviews — scraping della pagina pubblica del dealer AS24.

Fonte dei dati: https://www.autoscout24.it/concessionari/{slug}/recensioni
La pagina è Next.js SSR: il payload è già nel tag <script id="__NEXT_DATA__">
come JSON pulito, niente parsing di markup fragile. Chiave rilevante:
    data.props.pageProps.dealerInfoPage.ratings

Ogni pagina rende solo le prime ~10 review (le più recenti). La paginazione
oltre è client-side via XHR a endpoint AS24 interni non documentati → MVP
si accontenta delle ultime 10 + aggregato totale (reviewCount/ratingAverage),
stesso compromesso dell'API Google Places (New) che pure dà solo 5 review.

Persistenza:
- dealer_reviews (condivisa con Google) con source='autoscout' e meta jsonb
  che preserva i campi ricchi AS24 (grades per-aspetto, reply del dealer,
  topRating) non mappabili alle colonne standard.
- dealer_rating_sources (nuova) con una riga per (dealer_id, 'autoscout')
  per l'aggregato: rating_value, review_count, recommend_percentage, etc.

dealer_public.rating_value resta Google-only — la decisione su come combinare
le fonti lato FE/SEO viene rimandata a quando avremo dati reali.

Eleggibilità:
- autoscout_dealer_config.autoscout_public_slug valorizzato (è il gate)
- dealer_public.is_active OR almeno un dealer_site_public.is_active
  (stesso filtro di Google: sync solo dealer "vivi" nella piattaforma)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Eseguito come `python app/jobs/sync_autoscout_reviews.py` dalla cartella azurenet-engine:
# aggiungi root al sys.path prima degli import `app.*`
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import httpx
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal

BASE_DIR = _PROJECT_ROOT
load_dotenv(BASE_DIR / ".env")

logger = logging.getLogger(__name__)

AS24_DEALER_URL = "https://www.autoscout24.it/concessionari/{slug}/recensioni"
AS24_FETCH_REVIEWS_URL = "https://www.autoscout24.it/api/dealer-detail/fetch-reviews"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36"
)
NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.+?)</script>',
    re.DOTALL,
)
REQUEST_TIMEOUT = 30.0
# La pagina SSR serve i primi 10, poi l'UI fa POST a fetch-reviews con skip=10,20,…
# Il server risponde con array piatto da 10 review; a fine dataset ritorna [].
AS24_PAGE_SIZE = 10
# Hard cap difensivo: se per qualche motivo l'endpoint non termina mai,
# stoppiamo. 200 batch = 2000 review, ben oltre qualsiasi dealer reale.
AS24_MAX_PAGES = 200


# ─────────────────────────────────────────────
# Eleggibilità
# ─────────────────────────────────────────────

def eligible_dealers_for_autoscout_sync(db: Session) -> list[tuple[int, str, str | None]]:
    """
    Restituisce [(dealer_id, slug, expected_brand), ...] dei dealer che hanno:
    - autoscout_dealer_config.autoscout_public_slug valorizzato, E
    - dealer_public.is_active OR almeno dealer_site_public.is_active.

    expected_brand serve come sanity check SOFT: il worker confronta il
    customerName che AS24 mostra nella pagina con il brand_name/ragione_sociale
    del dealer nostro (fuzzy). Se lo slug è incollato male → warning, ma la
    sync non si ferma (decisione dell'operatore).

    NB: NON si può usare autoscout_dealer_config.customer_id per la verifica:
    quello è il Customer ID della Listing Creation API B2B (10 cifre), diverso
    dal customerId frontend del marketplace (8 cifre) che appare nel __NEXT_DATA__.

    Nota: autoscout_dealer_config.dealer_id è FK a utenti(id), mentre il
    dealer_id usato in dealer_reviews/dealer_rating_sources è dealer_public.id.
    Il join passa per dealer_public.owner_user_id = utenti.id.
    """
    rows = db.execute(
        text(
            """
            SELECT DISTINCT dp.id,
                   adc.autoscout_public_slug,
                   COALESCE(NULLIF(TRIM(dp.brand_name), ''),
                            NULLIF(TRIM(dp.nome_commerciale), ''),
                            NULLIF(TRIM(dp.ragione_sociale), '')) AS expected_brand
            FROM public.autoscout_dealer_config adc
            JOIN public.dealer_public dp ON dp.owner_user_id = adc.dealer_id
            WHERE NULLIF(TRIM(adc.autoscout_public_slug), '') IS NOT NULL
              AND (
                    COALESCE(dp.is_active, FALSE) IS TRUE
                 OR EXISTS (
                        SELECT 1
                        FROM public.dealer_site_public s
                        WHERE s.dealer_id = dp.id
                          AND COALESCE(s.is_active, FALSE) IS TRUE
                    )
              )
            ORDER BY dp.id
            """
        )
    ).fetchall()
    return [
        (int(r[0]), str(r[1]).strip(), (str(r[2]).strip() if r[2] else None))
        for r in rows
    ]


def _name_looks_like(as24_name: str | None, our_brand: str | None) -> bool:
    """
    Fuzzy "soft match" tra il customerName che AS24 mostra sulla pagina e il
    brand del dealer nostro. Normalizza (solo alnum lowercase) ed accetta se
    uno è substring dell'altro. Tollera mancanza di "srl"/"spa" e simili
    forme giuridiche che a volte non coincidono.
    """
    if not as24_name or not our_brand:
        return True  # niente da confrontare, non bloccare
    norm_a = re.sub(r"[^a-z0-9]", "", as24_name.lower())
    norm_b = re.sub(r"[^a-z0-9]", "", our_brand.lower())
    if not norm_a or not norm_b:
        return True
    # rimuovi coda "srl"/"sas"/"snc"/"spa"/"srls" in fondo per non far
    # fallire match quando il brand nostro è senza forma giuridica
    for suf in ("srls", "srl", "spa", "snc", "sas", "ss"):
        if norm_a.endswith(suf):
            norm_a = norm_a[: -len(suf)]
        if norm_b.endswith(suf):
            norm_b = norm_b[: -len(suf)]
    return norm_a in norm_b or norm_b in norm_a


# ─────────────────────────────────────────────
# Scraping
# ─────────────────────────────────────────────

def _fetch_as24_dealer_info(slug: str) -> dict[str, Any] | None:
    """
    Scarica la pagina recensioni e ritorna il dict `dealerInfoPage` dal __NEXT_DATA__.
    Contiene sia `ratings` (aggregato + primi 10 review) sia i metadati dealer
    (`customerId`, `customerName`, `slug`) usati per la validazione.
    """
    url = AS24_DEALER_URL.format(slug=slug)
    with httpx.Client(
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "it-IT,it;q=0.9",
        },
        follow_redirects=True,
        http2=True,
        timeout=REQUEST_TIMEOUT,
    ) as client:
        resp = client.get(url)

    if resp.status_code == 404:
        logger.warning("[AS24-REVIEWS] slug=%s -> 404, pagina non trovata", slug)
        return None
    if resp.status_code >= 400:
        logger.warning(
            "[AS24-REVIEWS] slug=%s -> HTTP %s", slug, resp.status_code
        )
        return None

    match = NEXT_DATA_RE.search(resp.text)
    if not match:
        logger.warning("[AS24-REVIEWS] slug=%s -> no __NEXT_DATA__ nel markup", slug)
        return None

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        logger.exception("[AS24-REVIEWS] slug=%s -> __NEXT_DATA__ JSON invalido", slug)
        return None

    return (
        data.get("props", {})
        .get("pageProps", {})
        .get("dealerInfoPage")
    )


def _fetch_as24_extra_reviews(
    customer_id: Any,
    slug: str,
    start_skip: int,
    expected_total: int | None,
) -> list[dict[str, Any]]:
    """
    Paginazione "Mostra altre" via POST a /api/dealer-detail/fetch-reviews.
    Continua a incrementare skip di AS24_PAGE_SIZE finché la risposta è vuota
    (fine del dataset) o abbiamo raggiunto expected_total. Hard cap AS24_MAX_PAGES
    per paracadute.

    Ogni pagina ritorna una lista piatta di oggetti con la stessa struttura
    dei review SSR (`reviewId`, `stars`, `created`, `name`, `grades`,
    `reviewText`, `replyText`, `replyCreated`, `topRating`).
    """
    if customer_id is None:
        return []
    all_extra: list[dict[str, Any]] = []
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Accept-Language": "it-IT,it;q=0.9",
        "Content-Type": "application/json",
        "Origin": "https://www.autoscout24.it",
        "Referer": AS24_DEALER_URL.format(slug=slug),
    }
    with httpx.Client(headers=headers, http2=True, timeout=REQUEST_TIMEOUT) as client:
        skip = start_skip
        for _ in range(AS24_MAX_PAGES):
            if expected_total is not None and skip >= expected_total:
                break
            try:
                resp = client.post(
                    AS24_FETCH_REVIEWS_URL,
                    json={"customerId": customer_id, "skip": skip},
                )
            except httpx.RequestError as exc:
                logger.warning(
                    "[AS24-REVIEWS] fetch-reviews skip=%s errore rete: %s — stop",
                    skip, exc,
                )
                break
            if resp.status_code >= 400:
                logger.warning(
                    "[AS24-REVIEWS] fetch-reviews skip=%s HTTP %s — stop",
                    skip, resp.status_code,
                )
                break
            try:
                batch = resp.json()
            except json.JSONDecodeError:
                logger.warning(
                    "[AS24-REVIEWS] fetch-reviews skip=%s risposta non JSON — stop",
                    skip,
                )
                break
            if not isinstance(batch, list) or not batch:
                break
            all_extra.extend(batch)
            if len(batch) < AS24_PAGE_SIZE:
                # Ultimo batch parziale: fine del dataset.
                break
            skip += AS24_PAGE_SIZE
    return all_extra


def _parse_as24_date(s: str | None) -> datetime | None:
    """AS24 usa il formato italiano 'dd.mm.yyyy'. Restituisce naive UTC date."""
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%d.%m.%Y")
    except ValueError:
        return None


def _compute_as24_review_hash(review_id: str) -> str:
    """Hash univoco globale. Prefisso 'autoscout:' evita collisioni con Google."""
    return hashlib.sha256(f"autoscout:{review_id}".encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────
# Persistenza
# ─────────────────────────────────────────────

def _upsert_rating_source(
    db: Session,
    dealer_id: int,
    slug: str,
    ratings: dict[str, Any],
    page_customer_id: Any = None,
    page_customer_name: str | None = None,
) -> None:
    """
    Upsert dell'aggregato AS24. Salviamo in meta anche il customerId/Name
    che AS24 espone nella pagina pubblica — utile per audit e per rilevare
    un domani se cambia (segnale che lo slug è stato riassegnato a un altro
    dealer AS24, caso raro ma possibile).
    """
    meta = {
        "gradesAverage": ratings.get("gradesAverage"),
        "ratingStars": ratings.get("ratingStars"),
        "as24CustomerId": page_customer_id,
        "as24CustomerName": page_customer_name,
    }
    db.execute(
        text(
            """
            INSERT INTO public.dealer_rating_sources
                (dealer_id, source, external_ref, rating_value, review_count,
                 recommend_percentage, last_sync, meta)
            VALUES
                (:dealer_id, 'autoscout', :slug, :rating_value, :review_count,
                 :recommend_pct, NOW(), :meta)
            ON CONFLICT (dealer_id, source) DO UPDATE SET
                external_ref         = EXCLUDED.external_ref,
                rating_value         = EXCLUDED.rating_value,
                review_count         = EXCLUDED.review_count,
                recommend_percentage = EXCLUDED.recommend_percentage,
                last_sync            = EXCLUDED.last_sync,
                meta                 = EXCLUDED.meta
            """
        ),
        {
            "dealer_id": dealer_id,
            "slug": slug,
            "rating_value": ratings.get("ratingAverage"),
            "review_count": ratings.get("reviewCount"),
            "recommend_pct": ratings.get("recommendPercentage"),
            "meta": json.dumps(meta, ensure_ascii=False),
        },
    )


def _insert_reviews(
    db: Session,
    dealer_id: int,
    reviews: list[dict[str, Any]],
) -> tuple[int, int]:
    """Insert idempotente via ON CONFLICT (review_hash). Ritorna (inserted, skipped)."""
    inserted = 0
    skipped = 0

    for r in reviews:
        review_id = r.get("reviewId")
        review_text = (r.get("reviewText") or "").strip()
        stars = r.get("stars")

        if not review_id or not review_text or stars is None:
            skipped += 1
            continue

        try:
            stars = int(stars)
        except (TypeError, ValueError):
            skipped += 1
            continue
        if stars < 1 or stars > 5:
            skipped += 1
            continue

        review_hash = _compute_as24_review_hash(str(review_id))

        meta = {
            "grades": r.get("grades"),
            "replyText": r.get("replyText") or None,
            "replyCreated": r.get("replyCreated"),
            "topRating": r.get("topRating"),
            "reviewId": review_id,
        }

        result = db.execute(
            text(
                """
                INSERT INTO public.dealer_reviews
                    (dealer_id, author_name, rating, review_text,
                     published_at, review_hash, source, meta)
                VALUES
                    (:dealer_id, :author_name, :rating, :review_text,
                     :published_at, :review_hash, 'autoscout', :meta)
                ON CONFLICT (review_hash) DO NOTHING
                """
            ),
            {
                "dealer_id": dealer_id,
                "author_name": (r.get("name") or "").strip() or None,
                "rating": stars,
                "review_text": review_text,
                "published_at": _parse_as24_date(r.get("created")),
                "review_hash": review_hash,
                "meta": json.dumps(meta, ensure_ascii=False),
            },
        )
        if result.rowcount and result.rowcount > 0:
            inserted += 1
        else:
            skipped += 1

    return inserted, skipped


# ─────────────────────────────────────────────
# Job entry points
# ─────────────────────────────────────────────

def sync_dealer_autoscout_reviews(
    dealer_id: int,
    slug: str,
    expected_brand: str | None = None,
) -> None:
    slug = (slug or "").strip()
    if not slug:
        logger.warning("[AS24-REVIEWS] dealer_id=%s senza slug, skip", dealer_id)
        return

    logger.info(
        "[AS24-REVIEWS] Sync dealer_id=%s slug=%s expected_brand=%r",
        dealer_id, slug, expected_brand,
    )

    info = _fetch_as24_dealer_info(slug)
    if info is None:
        return

    page_cid = info.get("customerId")
    page_name = info.get("customerName")

    # Soft sanity check: il nome dealer sulla pagina AS24 deve assomigliare
    # al brand/ragione_sociale nostro. Se no: warning ma procediamo comunque
    # — l'operatore ha inserito lo slug e noi non blocchiamo al primo sync.
    # Drift futuri sul customerId AS24 rispetto a quello salvato in meta
    # sono un segnale ulteriore (controllato fuori da qui in audit manuale).
    if expected_brand and not _name_looks_like(page_name, expected_brand):
        logger.warning(
            "[AS24-REVIEWS] NAME MISMATCH SOFT dealer_id=%s slug=%s "
            "expected_brand=%r as24_name=%r as24_cid=%s — verifica lo slug",
            dealer_id, slug, expected_brand, page_name, page_cid,
        )

    ratings = info.get("ratings") or {}
    ssr_reviews = ratings.get("reviews") or []
    review_count = ratings.get("reviewCount")

    # Se AS24 dichiara più di AS24_PAGE_SIZE review, paginiamo via XHR
    # per raccogliere lo storico completo (non solo gli ultimi 10).
    extra_reviews: list[dict[str, Any]] = []
    if page_cid is not None and (
        review_count is None or review_count > len(ssr_reviews)
    ):
        extra_reviews = _fetch_as24_extra_reviews(
            customer_id=page_cid,
            slug=slug,
            start_skip=len(ssr_reviews),
            expected_total=review_count,
        )

    all_reviews = ssr_reviews + extra_reviews

    db: Session = SessionLocal()
    try:
        _upsert_rating_source(
            db, dealer_id, slug, ratings,
            page_customer_id=page_cid,
            page_customer_name=page_name,
        )
        inserted, skipped = _insert_reviews(db, dealer_id, all_reviews)
        db.commit()
        logger.info(
            "[AS24-REVIEWS] DONE dealer_id=%s ssr=%s extra=%s "
            "inserted=%s skipped=%s rating=%s count=%s recommend=%s%% "
            "(as24_cid=%s name=%r)",
            dealer_id,
            len(ssr_reviews),
            len(extra_reviews),
            inserted,
            skipped,
            ratings.get("ratingAverage"),
            review_count,
            ratings.get("recommendPercentage"),
            page_cid,
            page_name,
        )
    except Exception:
        db.rollback()
        logger.exception("[AS24-REVIEWS] FAILED dealer_id=%s slug=%s", dealer_id, slug)
        raise
    finally:
        db.close()


def autoscout_reviews_sync_job() -> None:
    logger.info("[AS24-REVIEWS] Global sync start")

    db: Session = SessionLocal()
    try:
        targets = eligible_dealers_for_autoscout_sync(db)
        logger.info(
            "[AS24-REVIEWS] Eligible dealers (slug + dealer/site attivo): %s",
            len(targets),
        )
    finally:
        db.close()

    for dealer_id, slug, expected_brand in targets:
        try:
            sync_dealer_autoscout_reviews(dealer_id, slug, expected_brand)
        except Exception:
            # Log + prosegui con gli altri dealer. La function log già la traceback.
            continue

    logger.info("[AS24-REVIEWS] Global sync done")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def main():
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Sync AutoScout24 Reviews")
    parser.add_argument("--dealer-id", type=int, help="Dealer ID specifico (dealer_public.id)")
    parser.add_argument(
        "--slug",
        type=str,
        help="Override slug AS24 (es. per test iniziale senza toccare il DB)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Sync tutti i dealer eleggibili (slug + dealer/sito attivo)",
    )
    args = parser.parse_args()

    if args.dealer_id:
        db: Session = SessionLocal()
        try:
            row = db.execute(
                text(
                    """
                    SELECT adc.autoscout_public_slug,
                           COALESCE(NULLIF(TRIM(dp.brand_name), ''),
                                    NULLIF(TRIM(dp.nome_commerciale), ''),
                                    NULLIF(TRIM(dp.ragione_sociale), ''))
                    FROM public.autoscout_dealer_config adc
                    JOIN public.dealer_public dp ON dp.owner_user_id = adc.dealer_id
                    WHERE dp.id = :did
                    """
                ),
                {"did": args.dealer_id},
            ).fetchone()
            db_slug = row[0] if row else None
            db_brand = row[1] if row else None
        finally:
            db.close()

        slug = (args.slug or db_slug or "").strip()
        if not slug:
            logger.error(
                "[AS24-REVIEWS] dealer_id=%s: nessuno slug (né in --slug né in "
                "autoscout_dealer_config.autoscout_public_slug)",
                args.dealer_id,
            )
            return
        sync_dealer_autoscout_reviews(args.dealer_id, slug, db_brand)
    elif args.all:
        autoscout_reviews_sync_job()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
