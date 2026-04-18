"""
Rewrite news in stili editoriali alternativi (giornalistico, amichevole, tecnico).

Il rewrite "professionale" è il default e vive in news_articles.body_rewritten
(prodotto dal job rewrite_news_job). Questo worker genera versioni alternative
SOLO per gli stili effettivamente selezionati da almeno un dealer
(dealer_news_settings.rewrite_style), e le salva in news_article_rewrites come
cache condivisa: se 2 dealer scelgono "amichevole", l'articolo viene riscritto
una volta sola e servito a entrambi.

Se nessun dealer ha selezionato uno stile custom, il worker non fa chiamate AI.
"""

import logging
import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv
from sqlalchemy import text

from app.database import SessionLocal
from app.jobs.rewrite_news import rewrite_article_json

load_dotenv(_PROJECT_ROOT / ".env")

CUSTOM_STYLES = ("giornalistico", "amichevole", "tecnico")
BATCH_SIZE_PER_STYLE = 10  # articoli per stile per run

logging.basicConfig(level=logging.INFO)


def _active_custom_styles(db) -> list[str]:
    """Stili custom selezionati da almeno un dealer con news abilitate."""
    rows = db.execute(
        text("""
            SELECT DISTINCT rewrite_style
            FROM dealer_news_settings
            WHERE is_enabled = TRUE
              AND rewrite_style IS NOT NULL
              AND rewrite_style <> 'professionale'
        """)
    ).fetchall()
    return [r[0] for r in rows if r[0] in CUSTOM_STYLES]


def rewrite_news_styles_job(batch_size: int = BATCH_SIZE_PER_STYLE):
    logging.info("[REWRITE-STYLES] start")

    db = SessionLocal()
    try:
        styles = _active_custom_styles(db)
        if not styles:
            logging.info("[REWRITE-STYLES] nessuno stile custom attivo, skip")
            return

        logging.info(f"[REWRITE-STYLES] stili attivi: {styles}")

        for style in styles:
            rows = db.execute(
                text("""
                    SELECT a.id, a.title, a.body
                    FROM news_articles a
                    LEFT JOIN news_article_rewrites r
                           ON r.article_id = a.id AND r.style = :style
                    WHERE a.body IS NOT NULL
                      AND r.article_id IS NULL
                    ORDER BY a.published_at DESC
                    LIMIT :batch
                """),
                {"style": style, "batch": batch_size},
            ).fetchall()

            if not rows:
                logging.info(f"[REWRITE-STYLES] style={style}: niente da riscrivere")
                continue

            logging.info(f"[REWRITE-STYLES] style={style}: articoli da riscrivere {len(rows)}")

            done = 0
            failed = 0
            for row in rows:
                result = rewrite_article_json(row.title, row.body, style=style)

                if result and result.get("body"):
                    db.execute(
                        text("""
                            INSERT INTO news_article_rewrites (article_id, style, title, body)
                            VALUES (:article_id, :style, :title, :body)
                            ON CONFLICT (article_id, style) DO NOTHING
                        """),
                        {
                            "article_id": row.id,
                            "style":      style,
                            "title":      result.get("title") or row.title,
                            "body":       result["body"],
                        },
                    )
                    db.commit()
                    done += 1
                    logging.info(f"[REWRITE-STYLES] style={style} id={row.id} ok")
                else:
                    failed += 1
                    logging.warning(f"[REWRITE-STYLES] style={style} id={row.id} failed")

            logging.info(f"[REWRITE-STYLES] style={style} done={done} failed={failed}")

    except Exception:
        db.rollback()
        logging.exception("[REWRITE-STYLES] FAILED")
        raise
    finally:
        db.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Riscrivi news in stili custom (giornalistico/amichevole/tecnico)")
    parser.add_argument("--run", action="store_true", help="Esegui subito il job")
    parser.add_argument("--batch", type=int, default=BATCH_SIZE_PER_STYLE, help=f"Articoli per stile per run (default {BATCH_SIZE_PER_STYLE})")
    args = parser.parse_args()

    if args.run:
        rewrite_news_styles_job(batch_size=args.batch)
    else:
        print("Usa --run per eseguire il job")


if __name__ == "__main__":
    main()
