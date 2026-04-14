import logging
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone, timedelta
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import requests
from dotenv import load_dotenv
from sqlalchemy import text

from app.database import SessionLocal

load_dotenv(_PROJECT_ROOT / ".env")

APITUBE_API_KEY = os.getenv("APITUBE_API_KEY")
APITUBE_URL = "https://api.apitube.io/v1/news/everything"
ARTICLES_PER_RUN = 50
RETENTION_DAYS = 30
MIN_ENTITY_FREQUENCY = 2

logging.basicConfig(level=logging.INFO)


BODY_STRIP_PREFIXES = {
    # Boilerplate newsletter che precede l'articolo reale
    "autoappassionati.it": "scritte da chi le auto le guida per davvero. ",
}


def _slugify(txt: str) -> str:
    """Slug SEO italiano: lower + rimozione accenti + solo [a-z0-9-], max 80."""
    if not txt:
        return "articolo"
    normalized = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", normalized.lower())
    slug = re.sub(r"-+", "-", slug).strip("-")[:80].strip("-")
    return slug or "articolo"


def generate_unique_slug(db, title: str) -> str:
    """Genera slug univoco per news_articles: aggiunge suffisso -2, -3... se collisione."""
    base = _slugify(title)
    candidate = base
    n = 1
    while db.execute(
        text("SELECT 1 FROM news_articles WHERE slug = :s LIMIT 1"),
        {"s": candidate},
    ).fetchone():
        n += 1
        candidate = f"{base}-{n}"
    return candidate


def clean_body(body: str | None, source_domain: str) -> str | None:
    if not body:
        return None
    marker = BODY_STRIP_PREFIXES.get(source_domain)
    if marker:
        idx = body.find(marker)
        if idx != -1:
            body = body[idx + len(marker):]
    return body.strip() or None


def fetch_articles(domains: list[str]) -> list[dict]:
    if not APITUBE_API_KEY:
        logging.error("[NEWS] APITUBE_API_KEY non configurata")
        return []

    params = {
        "per_page": ARTICLES_PER_RUN,
        "language.code": "it",
        "source.domain": ",".join(domains),
        "sort.by": "published_at",
        "sort.order": "desc",
        "images.count": 1,
        "include_duplicates": "false",
    }

    response = requests.get(
        APITUBE_URL,
        headers={"X-API-Key": APITUBE_API_KEY},
        params=params,
        timeout=30,
    )

    if response.status_code != 200:
        logging.error(f"[NEWS] APITube error {response.status_code}: {response.text[:200]}")
        return []

    data = response.json()
    if data.get("status") != "ok":
        logging.error(f"[NEWS] APITube status not ok: {data}")
        return []

    return data.get("results", [])


def extract_automotive_brands(entities: list) -> list[dict]:
    brands = []
    for e in entities:
        if not isinstance(e, dict):
            continue
        if e.get("type") not in ("organization", "brand"):
            continue
        if e.get("frequency", 0) < MIN_ENTITY_FREQUENCY:
            continue
        meta = e.get("metadata") or {}
        sectors = meta.get("industry_sectors") or []
        if not any("auto" in s.lower() for s in sectors):
            continue
        brands.append({
            "entity_id": e["id"],
            "entity_name": e["name"],
            "frequency": e["frequency"],
        })
    return brands


def sync_news_job():
    logging.info("[NEWS] sync start")

    db = SessionLocal()
    try:
        rows = db.execute(
            text("SELECT domain FROM news_sources WHERE is_active = TRUE ORDER BY domain")
        ).fetchall()
        domains = [r[0] for r in rows]

        if not domains:
            logging.warning("[NEWS] nessuna fonte attiva in news_sources")
            return

        logging.info(f"[NEWS] fetch da {len(domains)} fonti: {domains}")

        articles = fetch_articles(domains)
        logging.info(f"[NEWS] articoli ricevuti: {len(articles)}")

        inserted = 0
        skipped = 0

        for a in articles:
            apitube_id = a.get("id")
            image = a.get("image") or ""
            source_domain = (a.get("source") or {}).get("domain", "")

            if not apitube_id or not source_domain:
                continue

            # Dedup: salta se già presente
            exists = db.execute(
                text("SELECT id FROM news_articles WHERE apitube_id = :aid"),
                {"aid": apitube_id},
            ).fetchone()

            if exists:
                skipped += 1
                continue

            # Inserisci articolo (slug stabile generato dal titolo)
            title_value = a.get("title", "")
            slug = generate_unique_slug(db, title_value)
            result = db.execute(
                text("""
                    INSERT INTO news_articles
                        (apitube_id, title, slug, href, image_url, published_at, source_domain, body)
                    VALUES
                        (:apitube_id, :title, :slug, :href, :image_url, :published_at, :source_domain, :body)
                    RETURNING id
                """),
                {
                    "apitube_id": apitube_id,
                    "title": title_value,
                    "slug": slug,
                    "href": a.get("href", ""),
                    "image_url": image,
                    "published_at": a.get("published_at"),
                    "source_domain": source_domain,
                    "body": clean_body(a.get("body"), source_domain),
                },
            )
            article_id = result.fetchone()[0]

            # Inserisci brand entities
            brands = extract_automotive_brands(a.get("entities") or [])
            for b in brands:
                db.execute(
                    text("""
                        INSERT INTO news_article_brands (article_id, entity_id, entity_name, frequency)
                        VALUES (:article_id, :entity_id, :entity_name, :frequency)
                        ON CONFLICT (article_id, entity_id) DO NOTHING
                    """),
                    {
                        "article_id": article_id,
                        "entity_id": b["entity_id"],
                        "entity_name": b["entity_name"],
                        "frequency": b["frequency"],
                    },
                )

            inserted += 1

        db.commit()
        logging.info(f"[NEWS] done — inserted={inserted} skipped={skipped}")

        # Cleanup articoli vecchi
        cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
        deleted = db.execute(
            text("DELETE FROM news_articles WHERE published_at < :cutoff"),
            {"cutoff": cutoff},
        ).rowcount
        db.commit()

        if deleted:
            logging.info(f"[NEWS] cleanup: {deleted} articoli rimossi (>{RETENTION_DAYS}gg)")

    except Exception:
        db.rollback()
        logging.exception("[NEWS] FAILED")
        raise
    finally:
        db.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Sync news da APITube")
    parser.add_argument("--run", action="store_true", help="Esegui subito il job")
    args = parser.parse_args()

    if args.run:
        sync_news_job()
    else:
        print("Usa --run per eseguire il job")


if __name__ == "__main__":
    main()
