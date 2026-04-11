"""
generate_glossary.py — genera ~190 termini di glossario automotive con gpt-5.

Approccio BATCHED: una chiamata per categoria, ogni chiamata chiede a gpt-5
di generare definizioni + esempi per tutti i termini di quella categoria
in un'unica JSON response. Molto più veloce ed economico di una call per
termine (14 calls totali vs 190).

Uso:
  python app/jobs/generate_glossary.py --run                       # tutte
  python app/jobs/generate_glossary.py --dry-run
  python app/jobs/generate_glossary.py --run --category finanziamento
  python app/jobs/generate_glossary.py --run --force               # rigenera
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
# GLOSSARIO — 14 categorie, ~190 termini
# voci con "*" sono premium (corsivo nell'input originale)
# ═════════════════════════════════════════════════════════════

GLOSSARY: dict[str, dict] = {
    "finanziamento": {
        "label": "Finanziamento e credito",
        "terms": [
            ("TAN", False), ("TAEG", False), ("Anticipo", False), ("Rata mensile", False),
            ("Maxirata", False), ("Rata balloon", False), ("Preammortamento", True),
            ("Leasing finanziario", False), ("Leasing operativo", False),
            ("Riscatto finale", False), ("Valore futuro garantito (VFG)", True),
            ("Cessione del quinto", False), ("Consolidamento debiti auto", True),
            ("Segnalazione CRIF", False), ("Surroga finanziamento auto", True),
            ("Penale estinzione anticipata", False), ("Finanziamento revolving auto", True),
            ("Piano di ammortamento", False), ("Indice Euribor applicato al leasing", True),
        ],
    },
    "noleggio": {
        "label": "Noleggio",
        "terms": [
            ("NLT", False), ("Noleggio breve termine", False), ("Rent-to-rent", True),
            ("Canone mensile", False), ("Canone all-inclusive", True),
            ("Anticipo NLT", False), ("Deposito cauzionale NLT", True),
            ("Chilometraggio contrattuale", False), ("Eccedenza chilometrica", True),
            ("Franchigia danni", True), ("Massimale copertura", False),
            ("Usura ammessa vs usura eccedente", True),
            ("Auto sostitutiva inclusa", False), ("Restituzione anticipata NLT", True),
            ("Rinnovo tacito contratto noleggio", True),
            ("Buyout NLT (acquisto a fine contratto)", True),
        ],
    },
    "tipologie": {
        "label": "Tipologie di veicolo",
        "terms": [
            ("Km0", False), ("Vettura dimostrativa (demo)", True),
            ("Breve percorrenza", False), ("Ex-aziendale", False),
            ("Ex-noleggio", False), ("Veicolo a uso promiscuo", True),
            ("Prima immatricolazione", False), ("Reimmatricolazione", True),
            ("Veicolo commerciale leggero (VCL)", False),
            ("Autocarro derivato da autovettura", True),
            ("Veicolo categoria N1", True),
            ("Veicolo d'epoca", False), ("Veicolo storico (30+ anni)", True),
            ("Veicolo da esportazione", False), ("Pre-registered vehicle", True),
        ],
    },
    "motorizzazioni": {
        "label": "Motorizzazioni e tecnologie",
        "terms": [
            ("Benzina", False), ("Diesel", False), ("GPL", False), ("Metano", False),
            ("Bifuel", True),
            ("Mild hybrid (MHEV)", False), ("Full hybrid (HEV)", False),
            ("Plug-in hybrid (PHEV)", False), ("BEV (full electric)", False),
            ("FCEV (fuel cell / idrogeno)", True), ("Range extender (REX)", True),
            ("Euro 6d", False), ("Euro 6d-temp", False),
            ("Omologazione WLTP", True), ("Ciclo WLTP vs NEDC", True),
            ("Cambio automatico", False),
            ("Cambio a doppia frizione (DCT)", True),
            ("Trazione integrale permanente", False),
            ("Trazione integrale disconnettibile", True),
        ],
    },
    "batteria": {
        "label": "Batteria e elettrico",
        "terms": [
            ("SOH (State of Health)", False), ("SOC (State of Charge)", True),
            ("kWh (capacità batteria)", False), ("Autonomia WLTP", False),
            ("Autonomia reale vs WLTP", True),
            ("Ricarica AC (lenta/wallbox)", False), ("Ricarica DC (fast charge)", False),
            ("Ricarica ultra-fast (HPC)", True), ("Wallbox", False),
            ("Colonnina pubblica", False),
            ("Vehicle-to-Grid (V2G)", True), ("Vehicle-to-Home (V2H)", True),
            ("Degrado batteria", False), ("Garanzia batteria separata", True),
            ("Certificato SOH da dealer", True), ("Battery passport (UE 2027)", True),
            ("Preconditioning batteria", True), ("Heat pump su EV", True),
        ],
    },
    "documenti": {
        "label": "Documenti e pratiche",
        "terms": [
            ("Carta di circolazione", False),
            ("Certificato di proprietà (ex libretto)", False),
            ("CRS (Certificato di Rottamazione e Sostituzione)", True),
            ("Visura PRA", False), ("Visura storica PRA", True),
            ("Passaggio di proprietà", False),
            ("Atto di vendita autenticato", True),
            ("Esportazione definitiva", False),
            ("Reimmatricolazione da estero", True),
            ("Radiazione PRA", False),
            ("Sospensione dalla circolazione", True),
            ("Targa di prova", True), ("Targa EE (esportazione)", True),
            ("SDI per fatturazione dealer", True),
            ("Procura a vendere", True),
        ],
    },
    "garanzie": {
        "label": "Garanzie e tutele legali",
        "terms": [
            ("Garanzia legale di conformità", False),
            ("D.Lgs. 170/2021", True),
            ("Garanzia convenzionale", False),
            ("Garanzia estesa (contrattuale)", False),
            ("Garanzia meccanica vs garanzia totale", True),
            ("Vizi occulti", False), ("Difetto di conformità", True),
            ("Onere della prova (inversione 12 mesi)", True),
            ("Diritto di recesso", False),
            ("Recesso da contratto a distanza (14 giorni)", True),
            ("Riparazione vs sostituzione vs riduzione prezzo", True),
            ("Azione redibitoria", True),
            ("Azione estimatoria (quanti minoris)", True),
        ],
    },
    "gravami": {
        "label": "Gravami e verifiche",
        "terms": [
            ("Fermo amministrativo", False),
            ("Fermo fiscale (Agenzia Entrate-Riscossione)", True),
            ("Ipoteca su veicolo", False), ("Pegno", False),
            ("Preavviso di fermo", True),
            ("Visura gravami PRA", False),
            ("Cancellazione fermo amministrativo", True),
            ("Auto fermate non iscrivibili", True),
            ("Sequestro penale", False),
            ("Sequestro conservativo", True),
            ("Furto e rinvenimento", False),
            ("Auto con telaio alterato", True),
            ("Controllo VIN (Vehicle Identification Number)", True),
            ("Verifica chilometri (Carfax Italia / Motorcheck)", True),
        ],
    },
    "tasse": {
        "label": "Tasse e costi di possesso",
        "terms": [
            ("Bollo auto", False),
            ("Calcolo bollo per kW", True),
            ("Superbollo (oltre 185 kW)", False),
            ("IPT (Imposta Provinciale di Trascrizione)", False),
            ("IPT prima vs seconda intestazione", True),
            ("IVA su usato (regime del margine)", False),
            ("Regime IVA ordinario vs margine", True),
            ("IVA intracomunitaria su auto nuove", True),
            ("Esenzione bollo elettrico", False),
            ("Esenzione bollo ibrido (variabile per regione)", True),
            ("Agevolazione bollo disabili", True),
            ("Costo immatricolazione", False),
            ("Diritti di motorizzazione", True),
        ],
    },
    "valutazione": {
        "label": "Valutazione e mercato",
        "terms": [
            ("Quotazione Eurotax", False),
            ("Quotazione Quattroruote Usato", True),
            ("Valore residuo (RV)", False),
            ("Valore futuro garantito", True),
            ("Deprezzamento", False),
            ("Curva di deprezzamento per segmento", True),
            ("Permuta", False), ("Ritiro istantaneo", False),
            ("Valutazione algoritmica (AI)", True),
            ("Delta tra quotazione e prezzo reale di mercato", True),
            ("Indice di liquidità del modello", True),
            ("Over-allowance (permuta sopravvalutata come leva commerciale)", True),
        ],
    },
    "assicurazione": {
        "label": "Assicurazione",
        "terms": [
            ("RCA", False), ("Massimale RCA minimo di legge", True),
            ("Kasko", False), ("Kasko parziale vs totale", True),
            ("Furto e incendio", False), ("Polizza cristalli", True),
            ("Polizza assistenza stradale", True),
            ("Polizza infortuni conducente", True),
            ("Bonus/Malus", False), ("Classe di merito CU", False),
            ("Attestato di rischio", False),
            ("Legge Bersani (ereditarietà classe)", True),
            ("Scoperto", False), ("Franchigia", False),
            ("Coassicurazione", True), ("Subrogazione assicurativa", True),
            ("Polizza inclusa in NLT: cosa copre davvero", True),
        ],
    },
    "manutenzione": {
        "label": "Manutenzione e revisione",
        "terms": [
            ("Tagliando ordinario", False), ("Tagliando straordinario", False),
            ("Service indicator vs scadenza temporale", True),
            ("Revisione periodica (4 anni poi ogni 2)", False),
            ("Centri revisione autorizzati vs privati", True),
            ("Bollino blu", False), ("Controllo gas di scarico", True),
            ("Filtro antiparticolato (FAP/DPF)", False),
            ("Rigenerazione FAP", True),
            ("AdBlue (SCR): cos'è e quando si ricarica", True),
            ("Richiamo ufficiale (recall)", False),
            ("Campagna di aggiornamento software (OTA vs officina)", True),
            ("Piano di manutenzione prepagato", True),
        ],
    },
    "ambientale": {
        "label": "Normativa ambientale e ZTL",
        "terms": [
            ("Classi emissioni Euro 1-6d", False),
            ("Euro 7 (tempistica e impatto)", True),
            ("ZTL", False), ("Area C (Milano)", False), ("Area B (Milano)", True),
            ("Targhe alterne", False),
            ("Blocchi diesel per fascia Euro", True),
            ("Ecobonus", False),
            ("Ecobonus 2025: fasce ISEE e tetti di prezzo", True),
            ("Rottamazione", False),
            ("Incentivo senza rottamazione", True),
            ("Contributo regionale integrativo", True),
            ("Detraibilità auto per professionisti", True),
            ("Deducibilità auto aziendali (limiti al 20%)", True),
        ],
    },
    "acquisto": {
        "label": "Processo di acquisto",
        "terms": [
            ("Proposta d'acquisto", False),
            ("Vincolo della proposta", True),
            ("Caparra confirmatoria", False),
            ("Caparra penitenziale", True),
            ("Compromesso", False),
            ("Ordine vs contratto definitivo", True),
            ("Consegna del veicolo", False),
            ("Verbale di consegna", True),
            ("Collaudo alla consegna", True),
            ("Periodo di prova (se previsto contrattualmente)", True),
            ("Diritto di recesso da contratto a distanza", False),
            ("Acquisto online con consegna a domicilio: tutele", True),
            ("Obbligo di informativa precontrattuale", True),
            ("Nullità clausole vessatorie", True),
        ],
    },
}


# ═════════════════════════════════════════════════════════════
# PROMPT GPT-5
# ═════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """Sei un lessicografo specializzato in automotive italiano con competenze giuridiche.
Scrivi voci di glossario concise, chiare, tecnicamente precise, con riferimenti normativi quando
rilevanti (Codice Civile, Codice del Consumo D.Lgs 206/2005, D.Lgs 170/2021, D.Lgs 21/2014,
Codice della Strada, normativa fiscale, GDPR). Lavori per un glossario pubblicato sui siti web
di concessionarie italiane."""

USER_PROMPT_TEMPLATE = """Genera voci di glossario automotive per la seguente categoria.

CATEGORIA: {category_label}
TERMINI: {terms_list}

REQUISITI per ogni termine:

1. **definition** (150-250 parole, testo piano, NON HTML):
   - Definizione chiara, professionale ma accessibile (non giuridichese)
   - Cosa significa il termine, quando si usa, a cosa serve
   - Se rilevante, 1 riferimento normativo esplicito (es. "ai sensi dell'art. 128 Codice del Consumo")
   - Puoi inserire `{{{{DEALER_NAME}}}}` al massimo 1 volta (opzionale, solo dove è naturale, es. "presso {{{{DEALER_NAME}}}} è possibile...")
   - NON usare HTML, solo testo piano con frasi ben strutturate

2. **example** (opzionale, max 100 parole):
   - Esempio pratico concreto con numeri/scenari reali se ha senso
   - Lascia stringa vuota se non applicabile

3. **see_also** (opzionale):
   - Array di termini CORRELATI di QUESTA STESSA lista che un lettore dovrebbe conoscere
   - Usa esattamente la stringa del termine come appare nella TERMINI list
   - Max 4 termini correlati
   - Vuoto se nessun collegamento forte

Output JSON valido con questa struttura:
{{
  "terms": [
    {{
      "term": "nome termine come ricevuto",
      "definition": "testo 150-250 parole...",
      "example": "esempio o stringa vuota",
      "see_also": ["altro termine", "altro"]
    }}
  ]
}}

Rispondi SOLO con il JSON, senza testo extra, senza code fences."""


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
    return t[:120] or "termine"


def generate_category_batch(client: OpenAI, category_label: str, terms: list[tuple[str, bool]]) -> list[dict] | None:
    terms_list = "\n".join(f"- {t[0]}" for t in terms)
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT_TEMPLATE.format(
                    category_label=category_label,
                    terms_list=terms_list,
                )},
            ],
            response_format={"type": "json_object"},
        )
    except Exception:
        logging.exception(f"[GLOSSARY] OpenAI call failed for category {category_label}")
        return None

    raw = resp.choices[0].message.content or ""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logging.error(f"[GLOSSARY] JSON parse failed: {raw[:400]}")
        return None

    items = data.get("terms") or []
    if not isinstance(items, list) or not items:
        logging.error(f"[GLOSSARY] no terms in response")
        return None
    return items


def run(category_filter: str | None = None, force: bool = False, dry_run: bool = False, limit: int | None = None):
    if not OPENAI_API_KEY:
        logging.error("OPENAI_API_KEY non configurata")
        sys.exit(1)

    categories = [(k, v) for k, v in GLOSSARY.items() if not category_filter or k == category_filter]
    total_terms = sum(len(v["terms"]) for _, v in categories)
    logging.info(f"[GLOSSARY] categorie={len(categories)} termini={total_terms}")

    if dry_run:
        for k, v in categories:
            print(f"## {v['label']} ({len(v['terms'])} termini)")
            for term, premium in v["terms"]:
                mark = " *" if premium else ""
                print(f"  - {term}{mark}")
        return

    client = OpenAI(api_key=OPENAI_API_KEY)
    done = 0
    skipped = 0
    failed = 0
    inserted_in_run = 0

    for cat_key, cat_data in categories:
        cat_label = cat_data["label"]
        cat_terms = cat_data["terms"]

        # Check existing: skip interi se tutti i termini di questa categoria già esistono e non force
        if not force:
            db = SessionLocal()
            try:
                existing_slugs = set()
                for term, _ in cat_terms:
                    slug = _slugify(term)
                    r = db.execute(
                        text("SELECT 1 FROM dealer_glossary WHERE slug = :s"),
                        {"s": slug},
                    ).fetchone()
                    if r:
                        existing_slugs.add(slug)
            finally:
                db.close()

            if len(existing_slugs) == len(cat_terms):
                logging.info(f"[GLOSSARY] skip categoria '{cat_key}' (tutti i termini già presenti)")
                skipped += len(cat_terms)
                continue

        logging.info(f"[GLOSSARY] categoria '{cat_key}' ({len(cat_terms)} termini) — chiamata gpt-5")
        items = generate_category_batch(client, cat_label, cat_terms)
        if not items:
            failed += len(cat_terms)
            continue

        # Insert ciascun termine (session per-iteration, evita idle-in-transaction)
        premium_map = {t[0]: t[1] for t in cat_terms}

        for idx, item in enumerate(items):
            term = (item.get("term") or "").strip()
            if not term:
                continue
            definition = (item.get("definition") or "").strip()
            example = (item.get("example") or "").strip() or None
            see_also_raw = item.get("see_also") or []
            is_premium = premium_map.get(term, False)

            # Normalizza see_also in slugs
            see_also_slugs: list[str] = []
            for rel in see_also_raw:
                rel_slug = _slugify(rel)
                if rel_slug and rel_slug != _slugify(term):
                    see_also_slugs.append(rel_slug)

            slug = _slugify(term)

            db = SessionLocal()
            try:
                existing = db.execute(
                    text("SELECT id FROM dealer_glossary WHERE slug = :s"),
                    {"s": slug},
                ).fetchone()

                if existing and not force:
                    skipped += 1
                    db.close()
                    continue

                if existing and force:
                    db.execute(
                        text("""
                            UPDATE dealer_glossary
                            SET definition = :definition,
                                example = :example,
                                see_also = :see_also,
                                manual_version = manual_version + 1,
                                updated_at = NOW()
                            WHERE id = :id
                        """),
                        {
                            "definition": definition,
                            "example": example,
                            "see_also": see_also_slugs or None,
                            "id": str(existing.id),
                        },
                    )
                else:
                    db.execute(
                        text("""
                            INSERT INTO dealer_glossary
                                (language, category, term, slug, definition, example,
                                 see_also, is_premium, sort_order, is_active)
                            VALUES
                                ('it', :category, :term, :slug, :definition, :example,
                                 :see_also, :is_premium, :sort_order, TRUE)
                        """),
                        {
                            "category": cat_key,
                            "term": term,
                            "slug": slug,
                            "definition": definition,
                            "example": example,
                            "see_also": see_also_slugs or None,
                            "is_premium": is_premium,
                            "sort_order": 100 + idx * 10,
                        },
                    )
                db.commit()
                done += 1
                inserted_in_run += 1
            except Exception:
                db.rollback()
                logging.exception(f"[GLOSSARY] DB insert failed for {slug}")
                failed += 1
            finally:
                db.close()

        logging.info(f"[GLOSSARY] categoria '{cat_key}' ok: {inserted_in_run} inseriti totali")

        if limit and done >= limit:
            break

    logging.info(f"[GLOSSARY] done={done} skipped={skipped} failed={failed}")


def main():
    parser = argparse.ArgumentParser(description="Genera glossario automotive con gpt-5 batched")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--category", help="Filtra per categoria (finanziamento, noleggio, ...)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    if not args.run and not args.dry_run:
        parser.print_help()
        sys.exit(0)

    run(
        category_filter=args.category,
        force=args.force,
        dry_run=args.dry_run,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
