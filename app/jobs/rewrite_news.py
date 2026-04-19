import json
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
BATCH_SIZE = 50  # articoli per run (coprire picchi APITube + smaltire backlog)

logging.basicConfig(level=logging.INFO)

# ─────────────────────────────────────────────────────────────
# PROMPT BUILDER
# ─────────────────────────────────────────────────────────────
# Il default "professionale" riscrive il body e ripulisce il titolo dalle
# citazioni della testata originale. Gli altri stili sono usati dal worker
# rewrite_news_styles_job e salvano su news_article_rewrites.

STYLE_LINES: dict[str, str] = {
    "professionale": "Tono formale e sobrio, terza persona, frasi piane, terminologia tecnica corretta.",
    "giornalistico": "Tono da cronaca automotive: incipit d'impatto, verbi attivi, ritmo dinamico, paragrafi brevi.",
    "amichevole":    "Tono colloquiale, dai del 'tu' al lettore, usa analogie quotidiane, riduci il gergo tecnico.",
    "tecnico":       "Approfondisci dati, cifre, specifiche e confronti numerici. Lessico da appassionati informati.",
}


def build_system_prompt(style: str) -> str:
    style_line = STYLE_LINES.get(style, STYLE_LINES["professionale"])
    return (
        "Sei un redattore editoriale automotive. Riscrivi titolo e corpo dell'articolo "
        "fornito seguendo queste regole obbligatorie:\n"
        "- Rispondi SOLO con un JSON valido con due chiavi stringa: \"title\" e \"body\".\n"
        "- Nessun preambolo, nessun commento, nessun testo fuori dal JSON.\n"
        "- \"title\": riscrivi in italiano, naturale, massimo 110 caratteri. "
        "NON menzionare testate, siti o fonti originali (niente \"Autoblog\", \"HDmotori\", "
        "\"Motor1\", \"AUTONEWS\", \"Al Volante\", \"Motorbox\", \"InsideEVs\", ecc.).\n"
        "- \"body\": riscrivi in italiano mantenendo TUTTI i fatti, dati e cifre originali. "
        "Lunghezza simile all'originale.\n"
        "- Rimuovi qualsiasi riferimento al sito sorgente, autori originali, call to action o link.\n"
        "- Inserisci il placeholder {{DEALER_NAME}} nel body 1 o 2 volte in modo naturale "
        "(es. \"come osserva {{DEALER_NAME}}\", \"per {{DEALER_NAME}} questo significa...\"). "
        "NON inserire il placeholder nel title.\n"
        f"- Stile: {style_line}"
    )


def rewrite_article_json(title: str | None, body: str, style: str = "professionale") -> dict | None:
    """Chiama OpenAI e ritorna {'title': str, 'body': str} o None su errore."""
    if not OPENAI_API_KEY:
        logging.error("[REWRITE] OPENAI_API_KEY non configurata")
        return None

    client = OpenAI(api_key=OPENAI_API_KEY)
    user_content = f"TITOLO ORIGINALE: {title or ''}\n\nCORPO ORIGINALE:\n{body}"

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": build_system_prompt(style)},
                {"role": "user",   "content": user_content},
            ],
            temperature=0.7,
            max_tokens=2000,
            response_format={"type": "json_object"},
        )
        raw = (response.choices[0].message.content or "").strip()
        if not raw:
            return None
        parsed = json.loads(raw)
        new_title = (parsed.get("title") or "").strip() or None
        new_body  = (parsed.get("body")  or "").strip() or None
        if not new_body:
            return None
        return {"title": new_title, "body": new_body}
    except Exception:
        logging.exception("[REWRITE] OpenAI call failed")
        return None


def rewrite_news_job(batch_size: int = BATCH_SIZE):
    """Rewrite default (stile professionale) su news_articles.title_rewritten / body_rewritten."""
    logging.info("[REWRITE] start")

    db = SessionLocal()
    try:
        # ORDER BY: pending più vecchi prima (backlog catch-up), poi nuovi.
        # Evita il bug storico: con ORDER BY DESC + BATCH piccolo, gli articoli
        # sopra-soglia di un giorno di picco rimanevano pending per sempre perché
        # il run successivo ripescava i nuovi.
        rows = db.execute(
            text("""
                SELECT id, title, body FROM news_articles
                WHERE body IS NOT NULL
                  AND body_rewritten IS NULL
                ORDER BY published_at ASC
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
            result = rewrite_article_json(row.title, row.body, style="professionale")

            if result and result.get("body"):
                db.execute(
                    text("""
                        UPDATE news_articles
                        SET body_rewritten  = :body_rewritten,
                            title_rewritten = :title_rewritten,
                            rewritten_at    = :rewritten_at
                        WHERE id = :id
                    """),
                    {
                        "id": row.id,
                        "body_rewritten":  result["body"],
                        "title_rewritten": result.get("title"),
                        "rewritten_at":    datetime.now(timezone.utc),
                    },
                )
                db.commit()
                done += 1
                logging.info(f"[REWRITE] id={row.id} ok ({len(result['body'])} chars body)")
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
    parser = argparse.ArgumentParser(description="Riscrivi news con AI (stile default professionale)")
    parser.add_argument("--run", action="store_true", help="Esegui subito il job")
    parser.add_argument("--batch", type=int, default=BATCH_SIZE, help=f"Articoli per run (default {BATCH_SIZE})")
    args = parser.parse_args()

    if args.run:
        rewrite_news_job(batch_size=args.batch)
    else:
        print("Usa --run per eseguire il job")


if __name__ == "__main__":
    main()
