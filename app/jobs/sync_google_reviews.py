import hashlib
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Eseguito come `python app/jobs/sync_google_reviews.py` dalla cartella azurenet-engine:
# Python non mette la root del progetto in sys.path → aggiungiamola prima degli import `app.*`
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import requests
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import DealerPublic, DealerReview

BASE_DIR = _PROJECT_ROOT
load_dotenv(BASE_DIR / ".env")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_PLACE_URL = "https://places.googleapis.com/v1/places/{}"

logging.basicConfig(level=logging.INFO)


def eligible_dealer_ids_for_review_sync(db: Session) -> list[int]:
    """
    Dealer con Place ID Google e considerati "da sincronizzare":
    - `dealer_public.is_active`, oppure
    - almeno una riga in `dealer_site_public` con `is_active` (sito pubblico acceso).

    Prima il job usava solo `dealer_public.is_active`: i dealer con solo sito attivo
    venivano esclusi pur avendo slug live.
    """
    rows = db.execute(
        text(
            """
            SELECT DISTINCT dp.id
            FROM public.dealer_public dp
            WHERE NULLIF(TRIM(dp.google_place_id), '') IS NOT NULL
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
    return [int(r[0]) for r in rows]


def google_reviews_sync_job():
    logging.info("[REVIEWS] Global sync start")

    db: Session = SessionLocal()

    try:
        dealer_ids = eligible_dealer_ids_for_review_sync(db)
        logging.info(f"[REVIEWS] Eligible dealers (place id + active dealer or active site): {len(dealer_ids)}")

    finally:
        db.close()

    for dealer_id in dealer_ids:
        sync_dealer_reviews(dealer_id)

    logging.info("[REVIEWS] Global sync done")

def compute_review_hash(author: str, rating: int, text: str) -> str:
    raw = f"{author}|{rating}|{text}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def sync_dealer_reviews(dealer_id: int):
    if not GOOGLE_API_KEY:
        logging.error("[REVIEWS] GOOGLE_API_KEY non configurata")
        return

    db: Session = SessionLocal()

    try:
        dealer = db.query(DealerPublic).filter(DealerPublic.id == dealer_id).first()

        if not dealer:
            logging.warning(f"[REVIEWS] Dealer {dealer_id} non trovato")
            return

        place_id = (dealer.google_place_id or "").strip()
        if not place_id:
            logging.info(
                f"[REVIEWS] Skip dealer_id={dealer_id}: nessun google_place_id — "
                "impostalo da DealerMax (Impostazioni / registrazione). Nessuna risoluzione automatica."
            )
            return

        # --------------------------------------------------
        # GEO (solo con place id esplicito dal dealer / admin)
        # --------------------------------------------------
        logging.info(f"[REVIEWS] GEO check dealer_id={dealer_id}")

        geo_url = f"https://places.googleapis.com/v1/places/{place_id}"

        headers = {
            "X-Goog-Api-Key": GOOGLE_API_KEY,
            "X-Goog-FieldMask": "location,googleMapsUri"
        }

        params = {
            "languageCode": "it"
        }

        res = requests.get(
            geo_url,
            headers=headers,
            params=params,
            timeout=10
        )

        res.raise_for_status()
        geo_data = res.json()

        logging.info(f"[REVIEWS] GEO RAW: {geo_data}")

        location = geo_data.get("location")

        if location:
            dealer.latitude = float(location.get("latitude"))
            dealer.longitude = float(location.get("longitude"))

        if geo_data.get("googleMapsUri"):
            dealer.google_maps_url = geo_data["googleMapsUri"]

        db.commit()

        logging.info(
            f"[REVIEWS] GEO updated lat={dealer.latitude} lng={dealer.longitude}"
        )

        logging.info(f"[REVIEWS] Sync dealer_id={dealer_id}")

        url = GOOGLE_PLACE_URL.format(place_id)

        # Places API (New): field mask obbligatorio (come GEO sopra). Query param `fields` non è supportato.
        headers_reviews = {
            "X-Goog-Api-Key": GOOGLE_API_KEY,
            "X-Goog-FieldMask": "rating,userRatingCount,reviews",
        }

        response = requests.get(
            url,
            params={"languageCode": "it"},
            headers=headers_reviews,
            timeout=10,
        )

        if response.status_code != 200:
            logging.warning(f"[REVIEWS] Google error {response.status_code}")
            return

        data = response.json()

        rating_value = data.get("rating")
        review_count = data.get("userRatingCount", 0)

        reviews_data = data.get("reviews", [])

        inserted = 0
        skipped = 0

        for r in reviews_data:

            text = (
                r.get("originalText", {}).get("text")
                or r.get("text", {}).get("text", "")
            )

            if not text:
                continue

            text = text.strip()
            author = r.get("authorAttribution", {}).get("displayName", "")
            rating = r.get("rating")

            review_hash = compute_review_hash(author, rating, text)

            exists = db.query(DealerReview).filter(
                DealerReview.review_hash == review_hash
            ).first()

            if exists:
                skipped += 1
                continue

            publish_time = r.get("publishTime")

            if publish_time:
                published_at = datetime.fromisoformat(
                    publish_time.replace("Z", "+00:00")
                )
            else:
                published_at = None

            review = DealerReview(
                dealer_id=dealer_id,
                author_name=author,
                author_photo=r.get("authorAttribution", {}).get("photoUri"),
                profile_url=r.get("authorAttribution", {}).get("uri"),
                rating=rating,
                review_text=text,
                published_relative=r.get("relativePublishTimeDescription"),
                published_at=published_at,
                review_hash=review_hash,
                source="google"
            )

            db.add(review)
            inserted += 1

        # Aggiorno aggregate rating
        if rating_value is not None:
            dealer.rating_value = rating_value
            dealer.review_count = review_count
            dealer.reviews_last_sync = datetime.utcnow()

        db.commit()

        logging.info(
            f"[REVIEWS] DONE dealer_id={dealer_id} inserted={inserted} skipped={skipped}"
        )

    except Exception as e:
        db.rollback()
        logging.exception(f"[REVIEWS] FAILED dealer_id={dealer_id}")
        raise

    finally:
        db.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Sync Google Reviews")
    parser.add_argument("--dealer-id", type=int, help="Dealer ID specifico")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Sync tutti i dealer eleggibili (place id + dealer attivo o sito attivo su dealer_site_public)",
    )
    parser.add_argument(
        "--all-with-place-id",
        action="store_true",
        help="Sync tutti i dealer con google_place_id (ignora flag attivo; utile per backfill one-shot)",
    )

    args = parser.parse_args()

    db: Session = SessionLocal()

    try:
        if args.dealer_id:
            sync_dealer_reviews(args.dealer_id)

        elif args.all:
            ids = eligible_dealer_ids_for_review_sync(db)
            logging.info(f"[REVIEWS] CLI --all: {len(ids)} dealer id(s)")
            for did in ids:
                sync_dealer_reviews(did)

        elif args.all_with_place_id:
            dealers = (
                db.query(DealerPublic)
                .filter(
                    DealerPublic.google_place_id.isnot(None),
                    DealerPublic.google_place_id != "",
                )
                .order_by(DealerPublic.id)
                .all()
            )
            logging.info(f"[REVIEWS] CLI --all-with-place-id: {len(dealers)} dealer(s)")
            for dealer in dealers:
                sync_dealer_reviews(dealer.id)

        else:
            print("Usa --dealer-id, --all oppure --all-with-place-id")

    finally:
        db.close()


if __name__ == "__main__":
    main()