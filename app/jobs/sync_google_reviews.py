import logging
import hashlib
import os
import requests
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.database import SessionLocal
from app.models import DealerPublic, DealerReview  # ti dirò sotto cosa aggiungere
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_PLACE_URL = "https://places.googleapis.com/v1/places/{}"

logging.basicConfig(level=logging.INFO)


def google_reviews_sync_job():
    logging.info("[REVIEWS] Global sync start")

    db: Session = SessionLocal()

    try:
        dealer_ids = [
            d.id
            for d in db.query(DealerPublic.id)
            .filter(DealerPublic.google_place_id.isnot(None))
            .all()
        ]

    finally:
        db.close()

    for dealer_id in dealer_ids:
        sync_dealer_reviews(dealer_id)

    logging.info("[REVIEWS] Global sync done")

def compute_review_hash(author: str, rating: int, text: str) -> str:
    raw = f"{author}|{rating}|{text}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def sync_dealer_reviews(dealer_id: int):
    db: Session = SessionLocal()

    if not GOOGLE_API_KEY:
        logging.error("[REVIEWS] GOOGLE_API_KEY non configurata")
        return

    try:
        dealer = db.query(DealerPublic).filter(DealerPublic.id == dealer_id).first()

        if not dealer:
            logging.warning(f"[REVIEWS] Dealer {dealer_id} non trovato")
            return

        if not dealer.google_place_id:
            logging.warning(f"[REVIEWS] Dealer {dealer_id} senza google_place_id")
            return

        logging.info(f"[REVIEWS] Sync dealer_id={dealer_id}")

        url = GOOGLE_PLACE_URL.format(dealer.google_place_id)

        params = {
            "languageCode": "it",
            "fields": "rating,userRatingCount,reviews"
        }

        headers = {
            "X-Goog-Api-Key": GOOGLE_API_KEY
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)

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
    parser.add_argument("--all", action="store_true", help="Sync tutti i dealer")

    args = parser.parse_args()

    db: Session = SessionLocal()

    try:
        if args.dealer_id:
            sync_dealer_reviews(args.dealer_id)

        elif args.all:
            dealers = (
                db.query(DealerPublic)
                .filter(DealerPublic.google_place_id.isnot(None))
                .all()
            )

            for dealer in dealers:
                sync_dealer_reviews(dealer.id)

        else:
            print("Usa --dealer-id oppure --all")

    finally:
        db.close()


if __name__ == "__main__":
    main()