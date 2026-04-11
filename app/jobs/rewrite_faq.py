"""
rewrite_faq.py — riscrittura AI delle FAQ dealer con review legale.

Per ogni FAQ attiva chiama OpenAI gpt-5 con un prompt che:
- Verifica la conformità alla normativa italiana (Codice del Consumo,
  D.Lgs 206/2005, Codice Civile, Codice della Strada, GDPR)
- Riscrive la risposta in forma estesa (300-500 parole) con riferimenti
  normativi espliciti ed esempi pratici
- Mantiene il placeholder {{DEALER_NAME}} inserito naturalmente
- Segnala problemi legali critici nella risposta corrente

Le riscritture vengono salvate in `dealer_faq_rewrites` come pending,
MAI applicate automaticamente. Un admin in DealerMax deve approvare
manualmente prima che finiscano in `dealer_faq.answer`.

Uso:
  python app/jobs/rewrite_faq.py --run                       # tutte le FAQ
  python app/jobs/rewrite_faq.py --run --category vendita    # una categoria
  python app/jobs/rewrite_faq.py --run --faq-id UUID         # una sola FAQ
  python app/jobs/rewrite_faq.py --run --limit 5             # primi 5
  python app/jobs/rewrite_faq.py --dry-run                   # anteprima
  python app/jobs/rewrite_faq.py --run --force               # rigenera anche se pending esiste
"""

import argparse
import json
import logging
import os
import sys
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
MODEL = "gpt-5"  # richiesto esplicitamente dall'utente

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SYSTEM_PROMPT = """Sei un consulente legale specializzato in diritto automotive italiano, con competenze su:
- Codice del Consumo (D.Lgs 206/2005, art. 128-135: garanzia legale di conformità 24 mesi)
- Codice Civile (vizi occulti art. 1490-1495, risoluzione contrattuale)
- D.Lgs 21/2014 (diritti del consumatore)
- Codice della Strada (passaggio di proprietà, cointestazione, permuta)
- GDPR e privacy (D.Lgs 196/2003 aggiornato)
- Normativa fiscale automotive (IVA, bolli, tassa proprietà)
- Normativa noleggio lungo e breve termine (contratti, franchigie, depositi cauzionali)
- Procedure ACI e Motorizzazione Civile

Lavori per un network di concessionarie auto in Italia. Le FAQ che analizzi vengono
pubblicate sui siti web dei dealer, quindi OGNI RISPOSTA DEVE ESSERE LEGALMENTE CORRETTA.
Informazioni sbagliate espongono il dealer a responsabilità civile e contestazioni."""

USER_PROMPT_TEMPLATE = """FAQ da riscrivere.

CATEGORIA: {category}

DOMANDA:
{question}

RISPOSTA ATTUALE:
{answer}

COMPITI:

1. **Audit legale** della risposta attuale:
   - Identifica errori giuridici, affermazioni non conformi, ambiguità pericolose
   - Segnala riferimenti normativi mancanti
   - Elenca i problemi nel campo `legal_issues` come array di stringhe
   - Se la risposta attuale è corretta, lascia l'array vuoto

2. **Riscrittura** completa della risposta:
   - Italiano professionale, tono chiaro ma accessibile (non giuridichese)
   - 300-500 parole
   - Riferimenti normativi espliciti quando rilevanti (es. "ai sensi dell'art. 132 del Codice del Consumo", "D.Lgs 21/2014")
   - 1-2 esempi pratici concreti
   - Struttura leggibile: paragrafi brevi, eventuale elenco puntato
   - Inserisci il placeholder `{{{{DEALER_NAME}}}}` 1 volta (massimo 2) in modo naturale (es. "Per le pratiche gestite da {{{{DEALER_NAME}}}}...")
   - Chiudi con un invito a contattare direttamente il dealer per casi specifici
   - NON inventare informazioni: se una norma cambia spesso o non sei sicuro, scrivi che "il cliente può verificare gli aggiornamenti normativi o contattare {{{{DEALER_NAME}}}} per dettagli attuali"

3. **Confidence level** `confidence` ∈ `high` / `medium` / `low`:
   - `high` se il contenuto normativo è consolidato e non ambiguo
   - `medium` se ci sono ambiguità o variabili territoriali
   - `low` se la domanda richiede valutazione caso-per-caso

4. **needs_human_review** `true` se:
   - La risposta attuale conteneva errori giuridici gravi
   - La domanda tocca ambiti in evoluzione normativa (es. e-mobility, incentivi statali)
   - Ci sono variabili regionali importanti
   - Confidence è `low`

Rispondi SOLO con un oggetto JSON valido della forma:

{{
  "legal_issues": ["..."],
  "rewritten_answer": "...",
  "confidence": "high|medium|low",
  "needs_human_review": true|false
}}

Non aggiungere testo prima o dopo il JSON. Non usare markdown code fences."""


def rewrite_faq(client: OpenAI, category: str, question: str, answer: str) -> dict | None:
    """Chiama OpenAI e ritorna il dict parsed dal JSON response, o None su errore."""
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT_TEMPLATE.format(
                    category=category, question=question, answer=answer
                )},
            ],
            response_format={"type": "json_object"},
        )
    except Exception:
        logging.exception(f"[FAQ-REWRITE] OpenAI call failed for category={category}")
        return None

    raw = resp.choices[0].message.content or ""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logging.error(f"[FAQ-REWRITE] JSON parse failed, raw={raw[:500]}")
        return None

    # Validazione minima
    if "rewritten_answer" not in data or not data["rewritten_answer"]:
        logging.error(f"[FAQ-REWRITE] missing rewritten_answer in response")
        return None
    if data.get("confidence") not in ("high", "medium", "low"):
        data["confidence"] = "medium"
    data.setdefault("legal_issues", [])
    data.setdefault("needs_human_review", False)
    return data


def run_rewrite(
    db,
    client: OpenAI,
    *,
    category: str | None = None,
    faq_id: str | None = None,
    limit: int | None = None,
    force: bool = False,
    dry_run: bool = False,
):
    # Query FAQ attive
    where = ["is_active = TRUE"]
    params: dict = {}
    if category:
        where.append("category = :category")
        params["category"] = category
    if faq_id:
        where.append("id = :faq_id")
        params["faq_id"] = faq_id

    if not force:
        where.append("""
            NOT EXISTS (
                SELECT 1 FROM dealer_faq_rewrites r
                WHERE r.faq_id = dealer_faq.id AND r.status = 'pending'
            )
        """)

    sql = f"""
        SELECT id, category, question, answer
        FROM dealer_faq
        WHERE {' AND '.join(where)}
        ORDER BY category, sort_order
    """
    if limit:
        sql += f" LIMIT {int(limit)}"

    rows = db.execute(text(sql), params).fetchall()

    if not rows:
        logging.info("[FAQ-REWRITE] nessuna FAQ da processare")
        return

    logging.info(f"[FAQ-REWRITE] FAQ da processare: {len(rows)}")

    if dry_run:
        for r in rows:
            logging.info(f"  [{r.category}] {r.question[:80]}")
        return

    done = 0
    flagged = 0
    failed = 0

    for r in rows:
        logging.info(f"[FAQ-REWRITE] processing id={r.id} category={r.category}")

        result = rewrite_faq(client, r.category, r.question, r.answer)
        if not result:
            failed += 1
            continue

        # Insert in dealer_faq_rewrites
        try:
            db.execute(
                text("""
                    INSERT INTO dealer_faq_rewrites
                        (faq_id, model, old_answer, new_answer, legal_issues,
                         confidence, needs_human_review, status)
                    VALUES
                        (:faq_id, :model, :old_answer, :new_answer, CAST(:legal_issues AS JSONB),
                         :confidence, :needs_human_review, 'pending')
                """),
                {
                    "faq_id": str(r.id),
                    "model": MODEL,
                    "old_answer": r.answer,
                    "new_answer": result["rewritten_answer"],
                    "legal_issues": json.dumps(result.get("legal_issues") or []),
                    "confidence": result["confidence"],
                    "needs_human_review": bool(result.get("needs_human_review")),
                },
            )
            db.commit()
        except Exception:
            db.rollback()
            logging.exception(f"[FAQ-REWRITE] DB insert failed for id={r.id}")
            failed += 1
            continue

        done += 1
        if result.get("needs_human_review"):
            flagged += 1

        logging.info(
            f"[FAQ-REWRITE] id={r.id} ok confidence={result['confidence']} "
            f"flag={result.get('needs_human_review')} "
            f"issues={len(result.get('legal_issues') or [])}"
        )

    logging.info(f"[FAQ-REWRITE] done={done} flagged={flagged} failed={failed}")


def main():
    if not OPENAI_API_KEY:
        logging.error("OPENAI_API_KEY non configurata")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Riscrittura AI FAQ con review legale")
    parser.add_argument("--run", action="store_true", help="Esegui la riscrittura")
    parser.add_argument("--dry-run", action="store_true", help="Mostra solo cosa verrebbe processato")
    parser.add_argument("--category", help="Filtra per categoria (vendita, noleggio_breve, noleggio_lungo, acquisto_veicoli)")
    parser.add_argument("--faq-id", help="Processa una sola FAQ per UUID")
    parser.add_argument("--limit", type=int, help="Numero massimo di FAQ da processare")
    parser.add_argument("--force", action="store_true", help="Rigenera anche se esiste già un rewrite pending")
    args = parser.parse_args()

    if not args.run and not args.dry_run:
        parser.print_help()
        sys.exit(0)

    client = OpenAI(api_key=OPENAI_API_KEY)
    db = SessionLocal()
    try:
        run_rewrite(
            db,
            client,
            category=args.category,
            faq_id=args.faq_id,
            limit=args.limit,
            force=args.force,
            dry_run=args.dry_run,
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
