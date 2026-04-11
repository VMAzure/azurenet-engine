import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import text

from app.database import SessionLocal

load_dotenv(_PROJECT_ROOT / ".env")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = "gpt-4o-mini"
BATCH_SIZE = 10  # articoli per run

logging.basicConfig(level=logging.INFO)

SYSTEM_PROMPT = """Sei un redattore editoriale automotive. Riscrivi l'articolo che ti viene fornito seguendo queste regole:

- Scrivi in italiano, tono professionale ma accessibile
- Mantieni tutti i fatti, dati e cifre originali
- Rimuovi qualsiasi riferimento al sito sorgente, autori, call to action o link
- Inserisci il placeholder {{DEALER_NAME}} in modo naturale 1 o 2 volte nel testo (es. "come spiega {{DEALER_NAME}}", "per {{DEALER_NAME}} questo significa...")
- Lunghezza simile all'originale
- Rispondi solo con il testo riscritto, senza introduzioni né note finali"""


def rewrite_article(body: str) -> str | None:
    if not OPENAI_API_KEY:
        logging.error("[REWRITE] OPENAI_API_KEY non configurata")
        return None

    client = OpenAI(api_key=OPENAI_API_KEY)

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": body},
            ],
            temperature=0.7,
            max_tokens=2000,
        )
        return response.choices[0].message.content.strip() or None
    except Exception:
        logging.exception("[REWRITE] OpenAI call failed")
        return None


def rewrite_news_job(batch_size: int = BATCH_SIZE):
    logging.info("[REWRITE] start")

    db = SessionLocal()
    try:
        rows = db.execute(
            text("""
                SELECT id, body FROM news_articles
                WHERE body IS NOT NULL
                  AND body_rewritten IS NULL
                ORDER BY published_at DESC
                LIMIT :batch
            """),
            {"batch": batch_size},
        ).fetchall()

        if not rows:
            logging.info("[REWRITE] nessun articolo da riscrivere")
            return

        logging.info(f"[REWRITE] articoli da riscrivere: {len(rows)}")

        done = 0
        failed = 0

        for row in rows:
            rewritten = rewrite_article(row.body)

            if rewritten:
                db.execute(
                    text("""
                        UPDATE news_articles
                        SET body_rewritten = :body_rewritten,
                            rewritten_at   = :rewritten_at
                        WHERE id = :id
                    """),
                    {
                        "id": row.id,
                        "body_rewritten": rewritten,
                        "rewritten_at": datetime.now(timezone.utc),
                    },
                )
                db.commit()
                done += 1
                logging.info(f"[REWRITE] id={row.id} ok ({len(rewritten)} chars)")
            else:
                failed += 1
                logging.warning(f"[REWRITE] id={row.id} failed")

        logging.info(f"[REWRITE] done={done} failed={failed}")

    except Exception:
        db.rollback()
        logging.exception("[REWRITE] FAILED")
        raise
    finally:
        db.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Riscrivi news con AI")
    parser.add_argument("--run", action="store_true", help="Esegui subito il job")
    parser.add_argument("--batch", type=int, default=BATCH_SIZE, help=f"Articoli per run (default {BATCH_SIZE})")
    args = parser.parse_args()

    if args.run:
        rewrite_news_job(batch_size=args.batch)
    else:
        print("Usa --run per eseguire il job")


if __name__ == "__main__":
    main()
