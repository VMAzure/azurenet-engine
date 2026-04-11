"""
generate_guides.py — genera 22 guide editoriali long-form con gpt-5.

One-shot: legge una lista di 22 topic predefiniti, chiama OpenAI per
ciascuno con prompt specializzato in diritto automotive italiano, e
inserisce il risultato direttamente in dealer_guide.

Le guide vengono marcate needs_human_review=true/false in base alla
risposta del modello; l'admin in DealerMax /admin/content?tab=guide
può poi modificarle, validarle, sospenderle, eliminarle.

Uso:
  python app/jobs/generate_guides.py --run                  # genera tutte
  python app/jobs/generate_guides.py --dry-run              # anteprima
  python app/jobs/generate_guides.py --run --limit 3        # test veloce
  python app/jobs/generate_guides.py --run --topic acquisto # per topic
"""

import argparse
import json
import logging
import os
import re
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
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = "gpt-5"

# ═════════════════════════════════════════════════════════════
# 22 TOPIC PREDEFINITI
# ═════════════════════════════════════════════════════════════

GUIDES: list[dict] = [
    # Acquisto (5)
    {
        "topic": "acquisto",
        "h1": "Come comprare un'auto usata garantita: la guida completa",
        "brief": "Copre tutto il funnel dall'intenzione all'acquisto: come scegliere, dove comprare, cosa controllare, documenti, garanzie, trattativa, consegna.",
    },
    {
        "topic": "acquisto",
        "h1": "Come valutare lo stato di un'auto usata prima di comprarla",
        "brief": "Checklist operativa: verifiche visive (carrozzeria, interni, gomme), verifiche meccaniche (motore, trasmissione), documentali (libretto, revisioni, storico), test drive.",
    },
    {
        "topic": "acquisto",
        "h1": "Come vendere la tua auto usata: 5 opzioni a confronto",
        "brief": "Confronto tra vendita privata, concessionaria, permuta, piattaforme online, ritiro istantaneo. Pro/contro, tempi, sicurezza, valore ottenibile.",
    },
    {
        "topic": "acquisto",
        "h1": "Permuta auto: come funziona e quando conviene davvero",
        "brief": "Funzionamento della permuta presso concessionario, valutazione, inquadramento contrattuale (art. 1552 c.c.), conguaglio, documenti, differenza da vendita privata.",
    },
    {
        "topic": "acquisto",
        "h1": "Auto usata o auto nuova: pro, contro e quando conviene davvero",
        "brief": "Confronto onesto con tabella pro/contro: svalutazione, garanzie, costo totale di possesso, disponibilità immediata, personalizzazione.",
    },
    # Garanzie (4)
    {
        "topic": "garanzie",
        "h1": "Cosa guardare nel contratto di acquisto auto usata",
        "brief": "Clausole essenziali: identificazione veicolo, prezzo, modalità pagamento, consegna, garanzia legale, vizi occulti, recesso, foro competente. Campanelli d'allarme.",
    },
    {
        "topic": "garanzie",
        "h1": "Garanzia legale di conformità: cos'è, quanto dura, cosa copre",
        "brief": "Disciplina completa D.Lgs 206/2005 art. 128-135 e D.Lgs 170/2021: durata 24 mesi, onere prova, rimedi disponibili, applicabilità all'usato (min 12 mesi), limiti.",
    },
    {
        "topic": "garanzie",
        "h1": "Cosa fare se l'auto usata ha difetti dopo l'acquisto",
        "brief": "Procedura pratica: denuncia entro 2 mesi, rimedi (riparazione/sostituzione/riduzione prezzo/risoluzione), documentazione, foro competente, ADR.",
    },
    {
        "topic": "garanzie",
        "h1": "Auto usata con vizi occulti: diritti e procedura",
        "brief": "Artt. 1490-1495 c.c., distinzione tra vizi occulti e difetti di conformità, termini decadenza e prescrizione, onere della prova, perizie, rimedi disponibili.",
    },
    # Finanziamento (3)
    {
        "topic": "finanziamento",
        "h1": "Finanziamento auto: come funziona e cosa controllare prima di firmare",
        "brief": "Tipi di finanziamento (classico, leasing, maxi-rata finale), costi totali, TAN/TAEG, documenti richiesti, istruttoria, recesso, diritti consumatore.",
    },
    {
        "topic": "finanziamento",
        "h1": "TAN e TAEG: cosa significano davvero e come confrontarli",
        "brief": "Definizioni chiare di TAN e TAEG, differenze, cosa includono, come leggere il SECCI, esempi numerici comparativi, errori comuni nella comparazione.",
    },
    {
        "topic": "finanziamento",
        "h1": "Anticipo zero sull'auto: conviene o è una trappola?",
        "brief": "Analisi onesta delle offerte a zero anticipo: cosa guardare, impatto sulla rata, TAEG reali, clausole nascoste, quando ha senso e quando no.",
    },
    # Noleggio (3)
    {
        "topic": "noleggio",
        "h1": "Noleggio a lungo termine (NLT): come funziona e a chi conviene",
        "brief": "NLT cos'è, canoni all-inclusive, durata, chilometraggio, servizi inclusi (bollo, assicurazione, manutenzione, soccorso), pro/contro vs acquisto, per privati e aziende.",
    },
    {
        "topic": "noleggio",
        "h1": "NLT vs leasing vs acquisto: confronto completo",
        "brief": "Tabella comparativa con 10+ parametri: proprietà, durata, rata, servizi inclusi, riscatto, flessibilità, fiscalità, rischi. Per privati e P.IVA separatamente.",
    },
    {
        "topic": "noleggio",
        "h1": "Noleggio breve termine: quando usarlo e quanto costa",
        "brief": "Quando conviene (trasferte, auto sostitutiva, occasioni), come si calcola il costo, franchigie e depositi cauzionali, cosa include, policy carburante, consigli.",
    },
    # Tecnico/Operativo (4)
    {
        "topic": "tecnico",
        "h1": "Come leggere la carta di circolazione auto",
        "brief": "Guida campo per campo: codici P, D, J, F1/F2, anno immatricolazione, categoria, emissioni, potenza, massa, cosa significa ogni sigla, esempi pratici.",
    },
    {
        "topic": "tecnico",
        "h1": "Passaggio di proprietà auto: procedura, costi e documenti",
        "brief": "Step-by-step: documenti necessari, dove farlo (ACI/STA, agenzia, notaio), costi (IPT, emolumenti, bollo), tempi, procedure online, cointestazioni, minori.",
    },
    {
        "topic": "tecnico",
        "h1": "Come controllare fermi amministrativi e ipoteche su un'auto usata",
        "brief": "Verifiche PRA obbligatorie prima dell'acquisto, come accedere (ACI, servizio online, app), costi della visura, cosa significa ogni voce, cosa fare in caso di gravami.",
    },
    {
        "topic": "tecnico",
        "h1": "Revisione auto: quando scade, cosa controllano, quanto costa",
        "brief": "Frequenza (4+2 anni per prima, biennale dopo), cosa viene controllato, costi (centro privato vs pubblico), sanzioni per revisione scaduta, tolleranza.",
    },
    # Elettrico (3)
    {
        "topic": "elettrico",
        "h1": "Auto elettrica usata: cosa controllare prima di comprarla",
        "brief": "Peculiarità dell'usato elettrico: stato batteria, garanzia residua batteria, SoH, storico ricariche, cavi e colonnine compatibili, aggiornamenti software, incentivi.",
    },
    {
        "topic": "elettrico",
        "h1": "Stato della batteria di un'auto elettrica usata: come verificarlo",
        "brief": "SoH (State of Health), come misurarlo, certificati indipendenti, app del costruttore, segnali di degrado, range reale vs dichiarato, garanzia batteria.",
    },
    {
        "topic": "elettrico",
        "h1": "Mild hybrid, full hybrid, plug-in: le differenze spiegate semplice",
        "brief": "Tre tecnologie con schema tecnico semplificato, quando conviene ciascuna in base ai km annui e al tipo di uso, consumi reali, costi di manutenzione.",
    },
]


# ═════════════════════════════════════════════════════════════
# PROMPT GPT-5
# ═════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """Sei un consulente legale e editoriale specializzato in automotive italiano.

Competenze giuridiche:
- Codice del Consumo (D.Lgs 206/2005, art. 128-135: garanzia legale 24 mesi)
- Codice Civile (vizi occulti artt. 1490-1495, permuta art. 1552 ss., risoluzione)
- D.Lgs 21/2014 (diritti del consumatore)
- D.Lgs 170/2021 (conformità beni di consumo)
- Codice della Strada (passaggio di proprietà, cointestazioni)
- GDPR e privacy
- Normativa fiscale automotive (IVA, bolli, IPT, tassa proprietà)
- Normativa noleggio lungo e breve termine
- Procedure ACI e Motorizzazione Civile

Scrivi guide editoriali per i siti web delle concessionarie italiane. Ogni guida
deve essere giuridicamente CORRETTA, pratica, con esempi concreti e riferimenti
normativi espliciti. Informazioni sbagliate espongono il dealer a responsabilità
civile."""

USER_PROMPT_TEMPLATE = """Scrivi una guida editoriale long-form sul seguente argomento.

TOPIC: {topic}
H1: {h1}
BRIEF: {brief}

REQUISITI:

1. **Output**: JSON valido con struttura esatta:
{{
  "h1": "...",
  "meta_description": "...",
  "body_html": "...",
  "legal_issues_in_brief": [],
  "confidence": "high|medium|low",
  "needs_human_review": true|false
}}

2. **meta_description** 140-160 caratteri, include beneficio chiaro per il lettore + nome {{{{DEALER_NAME}}}} quando naturale.

3. **body_html** 1500-2500 parole, HTML semplice con whitelist:
   - `<h2>` per sezioni principali (4-7 sezioni)
   - `<h3>` per sottosezioni (opzionale)
   - `<p>` per paragrafi (brevi, max 4-5 righe)
   - `<ul>`/`<ol>`/`<li>` per elenchi (obbligatori per checklist operative)
   - `<strong>` per termini chiave
   - `<em>` per enfasi leggera
   - `<blockquote>` per callout "Attenzione:" o avvisi importanti
   - NO `<a>`, `<img>`, `<script>`, `<style>`, `<div>`, `<span>`, attributi `class`/`style`

4. **Struttura obbligatoria** (adatta alle sezioni):
   - `<h2>Introduzione</h2>` breve, perché l'argomento è importante (2-3 paragrafi)
   - `<h2>Punti chiave / Cosa devi sapere</h2>` (elenco puntato)
   - `<h2>Come si fa / Cosa controllare</h2>` operativo, checklist
   - `<h2>Esempi pratici</h2>` almeno 2 esempi concreti con cifre/scenari
   - `<h2>Errori da evitare</h2>` con `<blockquote>` o lista warning
   - `<h2>Riferimenti normativi</h2>` se rilevanti (art. X del Codice, D.Lgs)
   - `<h2>Conclusioni</h2>` sintesi + CTA verso {{{{DEALER_NAME}}}}

5. **Placeholder `{{{{DEALER_NAME}}}}`**: inseriscilo 4-6 volte in modo naturale nel testo, distribuito su diverse sezioni (intro, esempi, conclusioni). Serve come difesa anti-duplicate-content tra dealer.

6. **Tono**: divulgativo-professionale, accessibile, italiano corretto, zero gergo legale opaco. Usa parole semplici quando possibile. Frasi brevi.

7. **Riferimenti normativi** (quando rilevanti): sempre espliciti con articolo (es. "ai sensi dell'art. 132 del Codice del Consumo", "D.Lgs 21/2014"). Non inventare norme. Se incerto, rimanda al dealer.

8. **Confidence**:
   - `high` = contenuto consolidato, normativa chiara e stabile
   - `medium` = alcune ambiguità o variabili territoriali/contrattuali
   - `low` = richiede valutazione caso per caso

9. **needs_human_review** = true se:
   - La guida tocca ambiti in evoluzione normativa (elettrico, incentivi)
   - Ci sono variabili regionali importanti (IPT, tariffe ACI)
   - Confidence è low

10. **legal_issues_in_brief**: eventuali note legali su aspetti del brief che andrebbero chiariti, o vuoto se tutto ok.

Rispondi SOLO con il JSON, senza testo extra, senza code fences markdown."""


# ═════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════

def _slugify(s: str) -> str:
    t = (s or "").strip().lower()
    t = re.sub(r"[àáâãäå]", "a", t)
    t = re.sub(r"[èéêë]", "e", t)
    t = re.sub(r"[ìíîï]", "i", t)
    t = re.sub(r"[òóôõö]", "o", t)
    t = re.sub(r"[ùúûü]", "u", t)
    t = re.sub(r"[^a-z0-9]+", "-", t)
    t = re.sub(r"-+", "-", t).strip("-")
    return t[:120] or "guida"


def _sanitize_html(html: str) -> str:
    """Rimuove tag non whitelisted per sicurezza (anche se il prompt li vieta)."""
    if not html:
        return ""
    # Rimuovi script, iframe, style, object, embed, form, input, button, meta, link
    html = re.sub(r"</?(?:script|iframe|style|object|embed|form|input|button|meta|link|div|span|a)[^>]*>", "", html, flags=re.IGNORECASE)
    # Rimuovi attributi class e style da tutti i tag (ma tieni i tag)
    html = re.sub(r'\s(?:class|style|onclick|onerror|onload)="[^"]*"', "", html, flags=re.IGNORECASE)
    return html.strip()


def generate_guide(client: OpenAI, guide_spec: dict) -> dict | None:
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT_TEMPLATE.format(
                    topic=guide_spec["topic"],
                    h1=guide_spec["h1"],
                    brief=guide_spec["brief"],
                )},
            ],
            response_format={"type": "json_object"},
        )
    except Exception:
        logging.exception(f"[GUIDE] OpenAI call failed for {guide_spec['h1']}")
        return None

    raw = resp.choices[0].message.content or ""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logging.error(f"[GUIDE] JSON parse failed: {raw[:400]}")
        return None

    if not data.get("body_html"):
        logging.error(f"[GUIDE] missing body_html")
        return None
    if not data.get("h1"):
        data["h1"] = guide_spec["h1"]

    data["body_html"] = _sanitize_html(data["body_html"])
    if data.get("confidence") not in ("high", "medium", "low"):
        data["confidence"] = "medium"
    data.setdefault("needs_human_review", False)
    return data


def run(topic_filter: str | None = None, limit: int | None = None, dry_run: bool = False, force: bool = False):
    if not OPENAI_API_KEY:
        logging.error("OPENAI_API_KEY non configurata")
        sys.exit(1)

    filtered = [g for g in GUIDES if not topic_filter or g["topic"] == topic_filter]
    if limit:
        filtered = filtered[:limit]

    logging.info(f"[GUIDE] da processare: {len(filtered)}")

    if dry_run:
        for g in filtered:
            print(f"  [{g['topic']}] {g['h1']}")
        return

    client = OpenAI(api_key=OPENAI_API_KEY)
    done = 0
    skipped = 0
    failed = 0
    flagged = 0

    for g in filtered:
        slug = _slugify(g["h1"])

        # Check existing (short-lived session)
        db = SessionLocal()
        try:
            existing = db.execute(
                text("SELECT id FROM dealer_guide WHERE slug = :s"),
                {"s": slug},
            ).fetchone()
        finally:
            db.close()

        if existing and not force:
            logging.info(f"[GUIDE] skip (già esiste): {slug}")
            skipped += 1
            continue

        # Chiamata OpenAI FUORI dalla transazione DB (~70 sec, superiore al timeout idle)
        logging.info(f"[GUIDE] generating: {g['h1']}")
        result = generate_guide(client, g)
        if not result:
            failed += 1
            continue

        # Session nuova per insert/update (short-lived)
        db = SessionLocal()
        try:
            if existing and force:
                db.execute(
                    text("""
                        UPDATE dealer_guide
                        SET h1 = :h1,
                            meta_description = :meta,
                            body_html = :body,
                            needs_human_review = :flag,
                            manual_version = manual_version + 1,
                            updated_at = NOW()
                        WHERE id = :id
                    """),
                    {
                        "h1": result["h1"],
                        "meta": result.get("meta_description"),
                        "body": result["body_html"],
                        "flag": bool(result.get("needs_human_review")),
                        "id": str(existing.id),
                    },
                )
            else:
                db.execute(
                    text("""
                        INSERT INTO dealer_guide
                            (language, topic, slug, h1, meta_description, body_html,
                             sort_order, is_active, needs_human_review)
                        VALUES
                            ('it', :topic, :slug, :h1, :meta, :body,
                             :sort, TRUE, :flag)
                    """),
                    {
                        "topic": g["topic"],
                        "slug": slug,
                        "h1": result["h1"],
                        "meta": result.get("meta_description"),
                        "body": result["body_html"],
                        "sort": 100 + done * 10,
                        "flag": bool(result.get("needs_human_review")),
                    },
                )
            db.commit()
            done += 1
            if result.get("needs_human_review"):
                flagged += 1
            logging.info(
                f"[GUIDE] ok: {slug} confidence={result['confidence']} "
                f"flag={result.get('needs_human_review')} "
                f"words={len(result['body_html'].split())}"
            )
        except Exception:
            db.rollback()
            logging.exception(f"[GUIDE] DB insert failed for {slug}")
            failed += 1
            continue
        finally:
            db.close()

    logging.info(f"[GUIDE] done={done} skipped={skipped} flagged={flagged} failed={failed}")


def main():
    parser = argparse.ArgumentParser(description="Generazione AI guide long-form con audit legale")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--topic", help="Filtra per topic (acquisto, garanzie, finanziamento, noleggio, tecnico, elettrico)")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--force", action="store_true", help="Rigenera anche se già esiste")
    args = parser.parse_args()

    if not args.run and not args.dry_run:
        parser.print_help()
        sys.exit(0)

    run(
        topic_filter=args.topic,
        limit=args.limit,
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    main()
