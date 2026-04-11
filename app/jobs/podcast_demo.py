"""
Demo standalone: genera 1 podcast veicolo a 2 voci con Gemini 2.5 TTS multi-speaker.

Uso:
    python app/jobs/podcast_demo.py [id_auto]

Se id_auto non specificato, usa quello demo. Salva 2 file in /tmp/:
- podcast_demo.wav (output diretto Gemini, PCM 24kHz mono)
- podcast_demo.mp3 (convertito con ffmpeg, 128k)

Pipeline:
1. Carica dati veicolo + dealer da v_apimax_detail
2. gpt-5 genera script JSON dialogo Marco/Lucia
3. Gemini 2.5 Flash TTS multi-speaker → PCM
4. ffmpeg → WAV + MP3
5. Stampa metadati e percorsi file
"""

from __future__ import annotations

import base64
import json
import logging
import os
import struct
import subprocess
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import text
from sqlalchemy.orm import Session

# Import path bootstrap
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.database import SessionLocal

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Windows console cp1252 fix: gpt-5 può restituire caratteri Unicode (es. non-breaking
# hyphen \u2011) che crashano print() sotto Windows. Riconfiguriamo stdout a UTF-8.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# Preferisci GEMINI_API_KEY (key abilitata per Generative Language API),
# fallback a GOOGLE_API_KEY se non presente
GOOGLE_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
# Fallback: prova a leggere GEMINI_API_KEY dal .env di azureimage-engine
if not os.getenv("GEMINI_API_KEY"):
    _img_env = Path(__file__).resolve().parent.parent.parent.parent / "azureimage-engine" / ".env"
    if _img_env.exists():
        for line in _img_env.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("GEMINI_API_KEY="):
                _k = line.split("=", 1)[1].strip().strip('"').strip("'")
                if _k:
                    GOOGLE_API_KEY = _k
                    break
GPT_MODEL = "gpt-5"
GEMINI_TTS_MODEL = "gemini-2.5-pro-preview-tts"
GEMINI_TTS_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_TTS_MODEL}:generateContent"
)

# Default demo vehicle: Porsche 911 Carrera Coupe, Scuderia 76 Milano
DEFAULT_VEHICLE_ID = "b45a37df-d796-4372-b7a4-4ae33eb5febb"

# Voci Gemini — scelte per CARISMA e AUTORITÀ da radio italiana
VOICE_MARCO = "Algenib"      # maschile, gravelly, carisma da broadcaster esperto
VOICE_LUCIA = "Laomedeia"    # femminile, upbeat, vivace, animata


SCRIPT_PROMPT_SYSTEM = """Sei autore di una trasmissione radiofonica italiana in prime time dedicata al
mondo dell'auto. Pensa a DeeJay + Radio 24 + Rai Radio1 Motori: informato ma
BRILLANTE, curioso ma RITMATO, colto ma MAI NOIOSO. Questo NON è un audiobook.
Questo è drive-time radio italiano che devi avere VOGLIA di ascoltare in
macchina nel traffico.

Ogni episodio dura 90-110 secondi ed è un dialogo vivace tra due conduttori
che si conoscono bene e si divertono a raccontare auto:

- MARCO: 45 anni, conduttore carismatico, ex tester di riviste. Sa tutto delle
  auto, ma lo racconta con CALORE e IRONIA, mai da professore. Si entusiasma
  davvero per le storie interessanti. Usa variazioni di ritmo: alterna frasi
  brevi incisive a frasi più distese. Non è calmo-flat, è vivo.

- LUCIA: 32 anni, brillante, rapida, curiosa per davvero. Reagisce con
  entusiasmo ai racconti di Marco ("Ah, questa non la sapevo!", "Davvero?",
  "Dai, allora è una storia pazzesca"). Le sue domande partono dalla vita
  reale di chi potrebbe volere quell'auto, NON dalla scheda tecnica.

I due si stuzzicano, interagiscono, hanno chimica. Non sono due lettori di
copione — sono due AMICI che parlano di auto alla radio con energia.

══════════════════════════════════════════════
FILOSOFIA DI PRODUZIONE
══════════════════════════════════════════════

Ogni episodio è incorniciato come una "scoperta dal campo": Marco e Lucia
raccontano un'auto interessante che HANNO TROVATO oggi da un dealer specifico.
Il framing non è "oggi vi parliamo della Lancia Ypsilon" (troppo da catalogo)
MA "oggi da Gamma Auto abbiamo trovato una Ypsilon che merita di essere
raccontata".

L'ascoltatore deve sentirsi come davanti a due amici che tornano da un
sopralluogo e raccontano con entusiasmo cosa hanno visto.

══════════════════════════════════════════════
REGOLA FERREA SULLE FONTI
══════════════════════════════════════════════

Il CONTENUTO EDITORIALE — cosa è l'auto, per chi è, come si vive, che
caratteristiche emozionali ha, tagline, highlights — viene ESCLUSIVAMENTE dal
blocco "CONTENUTO EDITORIALE" che ti verrà passato (text_tagline, text_short,
text_medium, TEXT_LONG, text_highlights, marketing_hooks, persona_target,
text_faq).

**text_long è la tua FONTE PRINCIPALE**. È già scritto con cura editoriale,
contiene tutto quello che serve: design, comfort, carattere, posizionamento,
per chi è. NON riscriverlo: DISTILLA dal text_long i 4-5 punti più
interessanti e rendili in dialogo vivace.

NON usare contenuti brand/storici generici (tipo "Lancia ha fatto questo e
quello"). La cornice è "cosa abbiamo trovato OGGI da questo dealer".

NON costruire descrizioni partendo da marca+modello+anno: quei dati sono già
integrati nei text_*. Usa il contenuto pronto, non lo rigenerare da zero.

══════════════════════════════════════════════
COSA NON DEVE APPARIRE (banditi)
══════════════════════════════════════════════

DATI TECNICI: niente cilindrata in cc, niente kW, niente CV, niente cambio,
niente trazione, niente alimentazione dettagliata, niente classe emissioni,
niente consumi, niente dotazioni (clima, Bluetooth, cerchi, LED, ABS, ESP,
ADAS, sedili, radio...). Questi dati stanno nella scheda veicolo sul sito,
non in un podcast radiofonico.

DATI COMMERCIALI: niente prezzo in cifre, niente finanziamento, niente
garanzia durate, niente passaggio di proprietà, niente IVA, niente permuta
dettagli, niente "senza finanziamento X / con finanziamento Y". L'unico
riferimento consentito al prezzo è indiretto e sobrio ("è un'ottima occasione",
"il prezzo è interessante" — MAI una cifra).

PAROLE BANDITE: "affare", "imperdibile", "scopri", "cerchi X?", "vuoi Y?",
"ti fa brillare", "non ti aspetti", "fantastic*", "perfett*", "incredibile",
"eccezionale", "ideale per" + lista, "scatto", "tutto quello che serve",
"da non perdere", domande retoriche da spot.

BUROCRATESE: "previo appuntamento", "verifica tecnica e documentale",
"modulo di prova", "supporto durevole", "in contratto". Non sono un contratto,
sono alla radio. Parla naturale.

HALLUCINATIONS: MAI inventare dati tecnici o commerciali. Se non li usi,
non esistono per l'episodio.

══════════════════════════════════════════════
STRUTTURA OBBLIGATORIA
══════════════════════════════════════════════

1) APERTURA DEALER-FIRST (sempre): Marco apre con una variante di
   "Oggi da [NomeDealer] abbiamo trovato una [modello] che...", oppure "In
   questo episodio vi portiamo da [NomeDealer], che ha in vetrina...", oppure
   "Siamo passati da [NomeDealer] e abbiamo visto una cosa interessante...".
   La cornice è un SOPRALLUOGO, non una presentazione di catalogo.

   Lucia reagisce con entusiasmo: "Ah, raccontami!", "Interessante, cos'è?",
   "Dai, fammi sentire!".

2) PRESENTAZIONE NATURALE: Marco nomina il modello + colore + chilometraggio
   in UNA frase sola, distillata dal text_short / text_tagline, con il tono
   di chi ha appena visto qualcosa dal vivo. Non snocciolare una lista.

3) RACCONTO DISTILLATO DAL text_long: prendi 3-4 punti VERI e interessanti
   dal text_long (design, comfort, carattere, vissuto quotidiano, contesto
   nel segmento). Rendili in 3-4 scambi vivaci tra Marco e Lucia. Usa
   highlights e marketing hooks per il tono. NON recitare il text_long:
   estraine il meglio in forma orale.

4) DOMANDA REALE 1: Lucia pone una delle text_faq (adattandola al parlato).
   Marco risponde con la sostanza della risposta, stile radiofonico.

5) DOMANDA REALE 2: seconda text_faq, stesso trattamento.

6) PER CHI È: breve battuta basata su persona_target. "Fa per chi...
   Meno per chi..." in forma sintetica, UN solo turno.

7) CHIUSURA SEMPLICE E CALOROSA (esattamente così, NON inventare):
   - Marco: una frase soft che rimanda al dealer. Es: "Se ti ha incuriosito,
     passa a vederla da [NomeDealer] a [Città], oppure dai un'occhiata sul
     sito [dominio]. È un'ottima occasione."
   - Lucia (chiusura): "Noi ci fermiamo qui. Grazie di averci ascoltato, ci
     sentiamo al prossimo episodio. Ciao ciao!" (oppure variante: "A presto!",
     "Buona strada a tutti!", "Arrivederci al prossimo podcast!")

   NIENTE rating Google citato in chiusura (troppo meta). NIENTE prezzo.
   NIENTE "corri", "affrettati", "solo per pochi". NIENTE CTA da televendita.

══════════════════════════════════════════════
REGOLE DI ENERGIA E RITMO (cruciali)
══════════════════════════════════════════════

- Il dialogo DEVE avere VITA. Non scrivere due monologhi alternati.
- Lucia REAGISCE spesso alle battute di Marco — piccole interiezioni genuine:
  "Ah, questa non la sapevo!", "Davvero?", "Dai!", "Interessante", "Eh però",
  "Aspetta aspetta", "Ma dai", "Certo!", "Assolutamente"
- Marco può usare interiezioni di connessione: "Senti questa", "Allora",
  "Ecco", "Pensa che", "E qui viene il bello", "Beh", "Ti dico di più"
- VARIA il ritmo: mescola frasi brevi (3-6 parole) con frasi medie. MAI tutte
  lunghe uguali, è sonnifero.
- Usa PAUSE DRAMMATICHE con il punto: "E sai cosa? Una cosa sola."
- OGNI TANTO un gioco di parole o una battuta leggera, se il tema lo permette
- I due si STUZZICANO con affetto ("Marco, ti stai emozionando..." / "Lucia,
  fammi finire!")

══════════════════════════════════════════════
REGOLE FORMATO TECNICO
══════════════════════════════════════════════

- Italiano parlato naturale, frasi brevi (max 22 parole)
- NIENTE asterischi, markdown, emoji, liste puntate, numeri romani, unicode
  speciali (\\u2011, \\u2014)
- I punti esclamativi sono PERMESSI (con parsimonia, per enfasi naturale)
- Marco e Lucia si chiamano per nome MASSIMO 2 volte totali
- 10-14 turni di dialogo (più turni = più ping-pong = più energia)
- Lunghezza dialogue: 1200-1700 caratteri, target ~100 secondi audio
- Km: scrivi solo se forniti e solo UNA volta, in forma parlata ("con poco
  più di 50mila chilometri", "con 80mila chilometri all'attivo")
- Anno: UNA volta, al momento dell'introduzione ("immatricolata nel 2023")

══════════════════════════════════════════════
OUTPUT STRICT JSON
══════════════════════════════════════════════

{
  "title": "<titolo giornalistico, max 70 char, evoca il modello non promette>",
  "description": "<descrizione editoriale 200-280 char, stile radiofonico>",
  "duration_estimate_sec": <int 90-115>,
  "dialogue": "Marco: ...\\nLucia: ...\\n...",
  "key_points": ["<3 concetti editoriali emersi>"],
  "target_audience": "<1 frase sobria su a chi può interessare>"
}
"""

USER_PROMPT_HEADER = """Genera l'episodio radiofonico seguendo RIGOROSAMENTE la filosofia e la struttura del system prompt. L'apertura deve essere DEALER-FIRST ("Oggi da {nome_commerciale} abbiamo trovato..." o variante equivalente).

══════════════════════════════════════════
DEALER (cornice narrativa e CTA)
══════════════════════════════════════════
- Nome dealer: {nome_commerciale}
- Città: {citta}
- Sito web: {primary_domain}

L'episodio si APRE dichiarando esplicitamente che la scoperta è di {nome_commerciale}.
La CHIUSURA rimanda a {nome_commerciale} ({citta}) o al sito {primary_domain}.

══════════════════════════════════════════
CONTENUTO EDITORIALE DEL VEICOLO (UNICA FONTE)
══════════════════════════════════════════

⚠️ IMPORTANTE: tutto quello che sai del veicolo (cos'è, cosa offre, per chi è,
marca, modello, anno, chilometri, colore, carattere) lo trovi QUI SOTTO. NON
costruire nulla da marca+modello+anno esterni: è già integrato in questi testi.
Il tuo lavoro è DISTILLARE questi testi in dialogo vivace, non crearne di nuovi.

Categoria: {auto_category}
Topic ricerca SEO: {semantic_topics}

── TAGLINE (hook veloce) ──
"{tagline}"

── TEXT_SHORT (riassunto editoriale, 1 paragrafo) ──
"{text_short}"

── TEXT_MEDIUM (descrizione più ampia, 1 paragrafo) ──
"{text_medium}"

── TEXT_LONG (⭐ FONTE PRINCIPALE — descrizione editoriale lunga, 6-8 paragrafi) ──
"\"\"\"
{text_long}
\"\"\""

Distilla da text_long i 3-4 punti più interessanti (design, comfort, carattere,
posizionamento, vissuto quotidiano, a chi è rivolto) e rendili in 3-4 scambi
tra Marco e Lucia. NON leggere testualmente il text_long. NON ripetere tutti i
paragrafi. Scegli i concetti più forti e rendili orali, con reazioni vive.

── HIGHLIGHTS (3 spunti narrativi concreti) ──
{highlights_block}

── PERSONA TARGET (a chi è rivolto) ──
"{persona_target}"

── MARKETING HOOKS (ispirazione tono — NON leggerli letterali) ──
{marketing_hooks_block}

══════════════════════════════════════════
DOMANDE REALI PER QUESTO VEICOLO (cuore dell'episodio)
══════════════════════════════════════════
Queste sono FAQ pensate specificamente per questo veicolo. Lucia le pone
adattandole al parlato naturale, Marco risponde con la sostanza, in stile
radiofonico vivace.

{car_faq_block}

══════════════════════════════════════════

Scrivi ora l'episodio. Ricorda:
- APERTURA: "Oggi da {nome_commerciale}..." o variante equivalente
- CONTENUTO: solo da TEXT_LONG + highlights + FAQ + persona_target
- CHIUSURA di Lucia: saluto semplice "grazie di averci ascoltato, al prossimo episodio"
- TONO: drive-time radio, vivace, carismatico, con reazioni vere
- NIENTE prezzo, NIENTE dati tecnici (cilindrata, kW, dotazioni), NIENTE brand history generica
"""


def fetch_vehicle_context(db: Session, vehicle_id: str) -> dict:
    """
    Carica tutto il contenuto editoriale per il podcast:
    - veicolo minimal (marca/modello/anno/km/colore) da v_apimax_detail
    - brand storia/curiosità da brand_content
    - contenuto AI pre-generato per l'auto da usato_ai_content (tagline, text_short,
      text_faq, text_highlights, marketing_hooks, persona_target)
    - dealer (nome, città, rating) da dealer_public
    - sito del dealer (primary_domain) da dealer_site_public
    """
    # Identificativi minimi del veicolo + dealer. Il CONTENUTO editoriale del
    # podcast viene TUTTO da usato_ai_content — non costruire descrizioni da
    # marca/modello (sono già nei text_*).
    veh = db.execute(
        text("""
            SELECT v.id_auto,
                   dp.id AS dealer_public_id, dp.owner_user_id,
                   dp.nome_commerciale, dp.citta, dp.provincia,
                   dp.rating_value, dp.review_count
            FROM v_apimax_detail v
            JOIN dealer_public dp ON dp.owner_user_id = v.dealer_id
            WHERE v.id_auto = :vid
        """),
        {"vid": vehicle_id},
    ).fetchone()
    if not veh:
        raise RuntimeError(f"Vehicle {vehicle_id} not found")
    veh = dict(veh._mapping)

    # Per-vehicle AI content (UNICA sorgente di contenuto editoriale del podcast).
    # Pesco TUTTI i text_* + metadati editoriali — il prompt poi decide cosa usare.
    ai_row = db.execute(
        text("""
            SELECT text_tagline, text_short, text_medium, text_long,
                   text_faq, text_highlights, marketing_hooks,
                   social_caption_short, persona_target, auto_category,
                   semantic_topics
            FROM usato_ai_content
            WHERE id_auto = :vid
        """),
        {"vid": vehicle_id},
    ).fetchone()
    veh["_ai"] = dict(ai_row._mapping) if ai_row else None

    # Sito dealer: primary_domain per il CTA
    primary_domain = None
    site_row = db.execute(
        text("""
            SELECT primary_domain
            FROM dealer_site_public
            WHERE dealer_id = :did
              AND is_active = TRUE
              AND is_primary = TRUE
            LIMIT 1
        """),
        {"did": veh["dealer_public_id"]},
    ).fetchone()
    if site_row and site_row.primary_domain:
        primary_domain = site_row.primary_domain
    veh["_primary_domain"] = primary_domain

    return veh


def _format_brand_block(brand: dict | None, marca: str, modello: str) -> str:
    if not brand:
        return (
            f"(Nessun contenuto editoriale specifico per {marca} nel database. "
            f"Puoi fare riferimento a conoscenza generale del modello {marca} {modello} "
            f"ma NON inventare date o fatti specifici.)"
        )
    parts = []
    if brand.get("storia"):
        parts.append(f"Storia del brand:\n{brand['storia'].strip()}")
    if brand.get("curiosita"):
        parts.append(f"Curiosità:\n{brand['curiosita'].strip()}")
    if brand.get("punti_di_forza"):
        parts.append(f"Punti di forza del brand:\n{brand['punti_di_forza'].strip()}")
    if brand.get("descrizione") and not parts:
        parts.append(f"Descrizione brand:\n{brand['descrizione'].strip()}")
    return "\n\n".join(parts) if parts else "(Brand content vuoto.)"


def _format_faq_block(faqs: list[dict]) -> str:
    if not faqs:
        return "(Nessuna FAQ disponibile.)"
    lines = []
    for i, f in enumerate(faqs, start=1):
        q = (f.get("question") or "").strip()
        a = (f.get("answer") or "").strip()
        if len(a) > 500:
            a = a[:500].rsplit(" ", 1)[0] + "…"
        lines.append(f"FAQ{i} [{f.get('category')}] Q: {q}\nA: {a}")
    return "\n\n".join(lines)


def _format_services(services: list[dict]) -> str:
    if not services:
        return "(Nessun servizio mappato)"
    return ", ".join(f"{s.get('title') or s.get('service_code')} ({s.get('service_code')})" for s in services)


def _present(v) -> bool:
    """True se il valore è utilizzabile (non None, non stringa vuota)."""
    if v is None:
        return False
    if isinstance(v, str) and not v.strip():
        return False
    if isinstance(v, (list, dict)) and not v:
        return False
    return True


def _phonetic_domain(domain: str) -> str:
    """
    Converte un dominio web in forma phonetic-friendly per TTS italiano.
    Gemini TTS tende a "saltare" trattini e legge i punti come pause.
    Esempio:
        www.gamma-auto.it      -> "gamma trattino auto punto it"
        scuderia76.dealer.azcore.it -> "scuderia76 punto dealer punto azcore punto it"
    """
    if not domain:
        return ""
    d = domain.strip().lower()
    if d.startswith("http://"):
        d = d[7:]
    if d.startswith("https://"):
        d = d[8:]
    if d.startswith("www."):
        d = d[4:]
    d = d.rstrip("/")
    parts = d.split(".")
    spoken_parts = []
    for p in parts:
        if "-" in p:
            spoken_parts.append(" trattino ".join(p.split("-")))
        else:
            spoken_parts.append(p)
    return " punto ".join(spoken_parts)


def _preprocess_for_tts(dialogue: str, primary_domain: str | None) -> str:
    """
    Sostituisce la forma canonica del dominio con la versione phonetic-friendly.
    Opera sia sulla forma completa (con www.) che su quella senza.
    """
    if not primary_domain:
        return dialogue
    phonetic = _phonetic_domain(primary_domain)
    if not phonetic:
        return dialogue
    # Candidati da sostituire (forma canonica + varianti)
    candidates = set()
    base = primary_domain.strip().lower().rstrip("/")
    # Rimuovi http(s)://
    for prefix in ("http://", "https://"):
        if base.startswith(prefix):
            base = base[len(prefix):]
    candidates.add(base)
    if base.startswith("www."):
        candidates.add(base[4:])
    else:
        candidates.add("www." + base)
    # Ordina per lunghezza desc per evitare sostituzioni parziali
    for c in sorted(candidates, key=len, reverse=True):
        # Case-insensitive replacement
        import re as _re
        pattern = _re.compile(_re.escape(c), _re.IGNORECASE)
        dialogue = pattern.sub(phonetic, dialogue)
    return dialogue


def _parse_jsonb(v):
    """usato_ai_content JSONB possono arrivare come lista/dict o come stringa JSON."""
    if v is None:
        return None
    if isinstance(v, (list, dict)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (json.JSONDecodeError, ValueError):
            return None
    return None


def _build_ai_blocks(ai: dict | None) -> dict:
    """Formatta i blocchi editoriali pre-generati dal db."""
    if not ai:
        return {
            "tagline": "(nessun tagline curato)",
            "text_short": "(nessuna narrativa breve)",
            "text_medium": "",
            "text_long": "",
            "highlights_block": "(nessun highlight disponibile)",
            "marketing_hooks_block": "(nessun marketing hook disponibile)",
            "persona_target": "(nessuna persona target dichiarata)",
            "car_faq_block": "(nessuna FAQ specifica curata per questo veicolo)",
            "auto_category": "",
            "semantic_topics": "",
        }

    tagline = ai.get("text_tagline") or ""
    text_short = ai.get("text_short") or ""
    text_medium = ai.get("text_medium") or ""
    text_long = ai.get("text_long") or ""
    persona_target = ai.get("persona_target") or ""
    auto_category = ai.get("auto_category") or ""

    topics = _parse_jsonb(ai.get("semantic_topics")) or []
    topics_str = ", ".join(topics) if topics else ""

    # Highlights JSONB → lista
    highlights = _parse_jsonb(ai.get("text_highlights")) or []
    if highlights:
        highlights_block = "\n".join(f"- {h}" for h in highlights)
    else:
        highlights_block = "(nessun highlight disponibile)"

    # Marketing hooks JSONB → lista
    hooks = _parse_jsonb(ai.get("marketing_hooks")) or []
    if hooks:
        marketing_hooks_block = "\n".join(f"- {h}" for h in hooks)
    else:
        marketing_hooks_block = "(nessuno)"

    # FAQ JSONB → lista di stringhe (possono essere "Q? A" concatenate o oggetti)
    raw_faq = _parse_jsonb(ai.get("text_faq")) or []
    if raw_faq:
        car_faq_lines = []
        for i, item in enumerate(raw_faq, start=1):
            if isinstance(item, dict):
                q = (item.get("question") or item.get("q") or "").strip()
                a = (item.get("answer") or item.get("a") or "").strip()
                if q and a:
                    car_faq_lines.append(f"FAQ {i}:\nDomanda: {q}\nRisposta: {a}")
                elif q:
                    car_faq_lines.append(f"FAQ {i}: {q}")
            elif isinstance(item, str):
                # Formato "Q? A" — tenta di spezzare sul primo "?"
                txt = item.strip()
                if "?" in txt:
                    q, _, rest = txt.partition("?")
                    q = q.strip() + "?"
                    a = rest.strip()
                    car_faq_lines.append(f"FAQ {i}:\nDomanda: {q}\nRisposta: {a}")
                else:
                    car_faq_lines.append(f"FAQ {i}: {txt}")
        car_faq_block = "\n\n".join(car_faq_lines) if car_faq_lines else "(nessuna FAQ specifica)"
    else:
        car_faq_block = "(nessuna FAQ specifica curata per questo veicolo)"

    return {
        "tagline": tagline or "(nessun tagline)",
        "text_short": text_short or "(nessuna narrativa breve)",
        "text_medium": text_medium or "",
        "text_long": text_long or "",
        "highlights_block": highlights_block,
        "marketing_hooks_block": marketing_hooks_block,
        "persona_target": persona_target or "(nessuna persona target)",
        "car_faq_block": car_faq_block,
        "auto_category": auto_category,
        "semantic_topics": topics_str,
    }


def _build_vehicle_facts(ctx: dict) -> tuple[str, list[str]]:
    """
    Costruisce il blocco "dati disponibili" (solo campi presenti) e la lista
    dei campi MANCANTI da bannare esplicitamente.
    """
    present_lines: list[str] = []
    missing: list[str] = []

    # Marca/modello sempre presenti
    mm = f"{ctx.get('marca') or ''} {ctx.get('modello') or ''}".strip()
    if mm:
        present_lines.append(f"Marca e modello: {mm}")

    if _present(ctx.get("allestimento")):
        present_lines.append(f"Allestimento: {ctx['allestimento']}")

    if _present(ctx.get("anno_immatricolazione")):
        present_lines.append(f"Anno immatricolazione: {ctx['anno_immatricolazione']}")

    if _present(ctx.get("km_certificati")):
        present_lines.append(f"Chilometri certificati: {int(ctx['km_certificati'])}")
    else:
        missing.append("Chilometraggio")

    if _present(ctx.get("previous_owner_count")):
        present_lines.append(f"Proprietari precedenti: {ctx['previous_owner_count']}")
    else:
        missing.append("Numero proprietari precedenti")

    # Cronologia tagliandi: booleano → sempre dichiarato ma distinguiamo stato
    if ctx.get("cronologia_tagliandi") is True:
        present_lines.append("Cronologia tagliandi: disponibile")
    else:
        missing.append("Cronologia tagliandi (stato non dichiarato)")

    if ctx.get("doppie_chiavi") is True:
        present_lines.append("Doppie chiavi: presenti")
    # se False/None, non menzionare né in present né in missing (poco rilevante)

    if _present(ctx.get("prezzo_vendita")):
        present_lines.append(f"Prezzo: {int(ctx['prezzo_vendita'])} euro")

    if _present(ctx.get("cilindrata")):
        present_lines.append(f"Cilindrata: {ctx['cilindrata']} cc")
    if _present(ctx.get("kw")) and _present(ctx.get("hp")):
        present_lines.append(f"Potenza: {ctx['kw']} kW, cioè {ctx['hp']} CV")
    elif _present(ctx.get("kw")):
        present_lines.append(f"Potenza: {ctx['kw']} kW")

    if _present(ctx.get("alimentazione")):
        present_lines.append(f"Alimentazione: {ctx['alimentazione']}")
    if _present(ctx.get("cambio")):
        present_lines.append(f"Cambio: {ctx['cambio']}")
    if _present(ctx.get("trazione")):
        present_lines.append(f"Trazione: {ctx['trazione']}")
    if _present(ctx.get("colore")):
        present_lines.append(f"Colore: {ctx['colore']}")

    if _present(ctx.get("classe_emissioni")):
        present_lines.append(f"Classe emissioni: {ctx['classe_emissioni']}")
    else:
        missing.append("Classe emissioni (Euro 4/5/6/6D — NON assumere)")

    if _present(ctx.get("categoria")):
        present_lines.append(f"Categoria: {ctx['categoria']}")
    if _present(ctx.get("segmento")):
        present_lines.append(f"Segmento: {ctx['segmento']}")

    facts_block = "\n".join(f"- {line}" for line in present_lines) if present_lines else "(Nessun dato tecnico disponibile)"
    missing_block = (
        "\n".join(f"- {m}" for m in missing)
        if missing
        else "(Nessun dato mancante)"
    )
    return facts_block, missing_block


def _build_dealer_voice(ctx: dict) -> str:
    """Descrizione + precisazioni + ultimo intervento, solo se presenti."""
    parts: list[str] = []
    if _present(ctx.get("descrizione")):
        parts.append(f"Descrizione scritta dal dealer:\n\"\"\"\n{ctx['descrizione'].strip()}\n\"\"\"")
    if _present(ctx.get("precisazioni")):
        parts.append(f"Precisazioni / note:\n\"\"\"\n{ctx['precisazioni'].strip()}\n\"\"\"")
    if _present(ctx.get("descrizione_ultimo_intervento")):
        parts.append(f"Ultimo intervento:\n\"\"\"\n{ctx['descrizione_ultimo_intervento'].strip()}\n\"\"\"")
    if not parts:
        return "(Il dealer non ha fornito testo libero per questo veicolo. Marco può solo lavorare sui dati oggettivi e sulla reputazione del dealer.)"
    return "\n\n".join(parts)


def _build_dealer_block(ctx: dict, services_str: str) -> str:
    """Authority signals solo se presenti."""
    lines = []
    if _present(ctx.get("nome_commerciale")):
        name_line = f"Nome: {ctx['nome_commerciale']}"
        if _present(ctx.get("citta")):
            name_line += f" — {ctx['citta']}"
            if _present(ctx.get("provincia")):
                name_line += f" ({ctx['provincia']})"
        lines.append(name_line)

    if _present(ctx.get("founded_year")):
        lines.append(f"Fondato nel {ctx['founded_year']}")

    if _present(ctx.get("rating_value")) and _present(ctx.get("review_count")):
        lines.append(f"Rating medio Google: {ctx['rating_value']}/5 su {ctx['review_count']} recensioni")

    if services_str and services_str != "(Nessun servizio mappato)":
        lines.append(f"Servizi attivi sul sito del dealer: {services_str}")

    return "\n".join(lines) if lines else "(Dati dealer minimi)"


def generate_script(client: OpenAI, ctx: dict) -> dict:
    # AI content pre-generato: UNICA sorgente del contenuto editoriale
    ai_blocks = _build_ai_blocks(ctx.get("_ai"))

    # Dealer + sito (cornice narrativa e CTA)
    nome_commerciale = ctx.get("nome_commerciale") or "il dealer"
    citta = ctx.get("citta") or ""
    primary_domain = ctx.get("_primary_domain") or "(sito non configurato)"

    user_prompt = USER_PROMPT_HEADER.format(
        auto_category=ai_blocks.get("auto_category") or "",
        semantic_topics=ai_blocks.get("semantic_topics") or "",
        tagline=ai_blocks["tagline"],
        text_short=ai_blocks["text_short"],
        text_medium=ai_blocks.get("text_medium") or "",
        text_long=ai_blocks.get("text_long") or "(text_long non disponibile)",
        highlights_block=ai_blocks["highlights_block"],
        marketing_hooks_block=ai_blocks["marketing_hooks_block"],
        persona_target=ai_blocks["persona_target"],
        car_faq_block=ai_blocks["car_faq_block"],
        nome_commerciale=nome_commerciale,
        citta=citta,
        primary_domain=primary_domain,
    )

    logging.info(f"[PODCAST] gpt-5 generating script (prompt={len(user_prompt)} char)…")
    resp = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {"role": "system", "content": SCRIPT_PROMPT_SYSTEM},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
    )
    raw = resp.choices[0].message.content or ""
    data = json.loads(raw)
    logging.info(
        f"[PODCAST] script ok: title={data.get('title')!r} "
        f"duration_est={data.get('duration_estimate_sec')}s "
        f"dialogue={len(data.get('dialogue') or '')} char"
    )
    return data


def synthesize_with_gemini(dialogue_text: str) -> bytes:
    """
    Chiama Gemini 2.5 Flash TTS multi-speaker.
    Returns: raw PCM 16-bit little-endian, 24000 Hz, mono.
    """
    # Istruzione di stile FORTE: high-energy drive-time radio, non audiobook piatto.
    # Gemini TTS multi-speaker risponde bene a direttive di stile nel testo.
    style_prefix = (
        "TTS the following Italian podcast dialogue with HIGH ENERGY, CHARISMA and "
        "broadcaster confidence. This is drive-time motoring radio, NOT a slow audiobook. "
        "Speak at a slightly faster-than-average pace with natural variation. "
        "Marco is a warm, charismatic, confident broadcaster: emphasizes key words, "
        "sounds genuinely enthusiastic about cars, uses natural pauses for emphasis. "
        "Lucia is bright, animated, curious: her questions rise with real interest, "
        "her reactions are alive — she sounds delighted to be there. "
        "Both voices are vivid, expressive, engaged — absolutely NOT flat, measured, "
        "or clinical. Think Italian prime-time radio show, full of personality.\n\n"
        "Dialogue:\n"
    )
    payload = {
        "contents": [{
            "parts": [{
                "text": style_prefix + dialogue_text
            }]
        }],
        "generationConfig": {
            "responseModalities": ["AUDIO"],
            "speechConfig": {
                "multiSpeakerVoiceConfig": {
                    "speakerVoiceConfigs": [
                        {
                            "speaker": "Marco",
                            "voiceConfig": {
                                "prebuiltVoiceConfig": {"voiceName": VOICE_MARCO}
                            },
                        },
                        {
                            "speaker": "Lucia",
                            "voiceConfig": {
                                "prebuiltVoiceConfig": {"voiceName": VOICE_LUCIA}
                            },
                        },
                    ]
                }
            },
        },
    }

    logging.info(f"[PODCAST] Gemini TTS multi-speaker call (model={GEMINI_TTS_MODEL})…")
    with httpx.Client(timeout=180.0) as client:
        r = client.post(
            GEMINI_TTS_URL,
            json=payload,
            headers={
                "Content-Type": "application/json",
                # Pass key via header to avoid logging it in URL
                "x-goog-api-key": GOOGLE_API_KEY or "",
            },
        )
    if r.status_code >= 400:
        # Stampa solo metodo + status, niente URL (che conterrebbe la key se loggato dal libreria)
        logging.error(f"[PODCAST] Gemini error {r.status_code}: {r.text[:600]}")
        raise RuntimeError(f"Gemini TTS HTTP {r.status_code}")

    data = r.json()
    try:
        b64_audio = data["candidates"][0]["content"]["parts"][0]["inlineData"]["data"]
    except (KeyError, IndexError) as e:
        logging.error(f"[PODCAST] Unexpected response shape: {json.dumps(data)[:600]}")
        raise RuntimeError(f"Cannot parse Gemini TTS response: {e}")

    pcm_bytes = base64.b64decode(b64_audio)
    logging.info(f"[PODCAST] Gemini ok: {len(pcm_bytes)} bytes PCM ({len(pcm_bytes) / (24000 * 2):.1f}s mono 24kHz)")
    return pcm_bytes


def pcm_to_wav(pcm_bytes: bytes, sample_rate: int = 24000) -> bytes:
    """Wrap PCM 16-bit mono in WAV header."""
    num_channels = 1
    bits_per_sample = 16
    byte_rate = sample_rate * num_channels * bits_per_sample // 8
    block_align = num_channels * bits_per_sample // 8
    data_size = len(pcm_bytes)
    fmt_chunk = struct.pack(
        "<4sIHHIIHH",
        b"fmt ", 16, 1, num_channels, sample_rate, byte_rate, block_align, bits_per_sample,
    )
    data_chunk = struct.pack("<4sI", b"data", data_size) + pcm_bytes
    riff = struct.pack("<4sI4s", b"RIFF", 36 + data_size, b"WAVE")
    return riff + fmt_chunk + data_chunk


def wav_to_mp3(wav_path: Path, mp3_path: Path):
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(wav_path), "-codec:a", "libmp3lame",
         "-b:a", "128k", "-ar", "24000", str(mp3_path)],
        check=True,
        capture_output=True,
    )


def main():
    if not OPENAI_API_KEY:
        logging.error("OPENAI_API_KEY non configurata")
        sys.exit(1)
    if not GOOGLE_API_KEY:
        logging.error("GOOGLE_API_KEY non configurata in azurenet-engine/.env")
        sys.exit(1)

    vehicle_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_VEHICLE_ID
    logging.info(f"[PODCAST] target vehicle: {vehicle_id}")

    db = SessionLocal()
    try:
        ctx = fetch_vehicle_context(db, vehicle_id)
    finally:
        db.close()

    logging.info(
        f"[PODCAST] target dealer: {ctx['nome_commerciale']} ({ctx['citta']})"
    )
    ai = ctx.get("_ai") or {}
    logging.info(
        f"[PODCAST] editorial sources: ai_content={bool(ai)}, "
        f"tagline={bool(ai.get('text_tagline'))}, "
        f"text_short={bool(ai.get('text_short'))}, "
        f"text_medium={bool(ai.get('text_medium'))}, "
        f"text_long={bool(ai.get('text_long'))} ({len(ai.get('text_long') or '')} char), "
        f"text_faq_count={len(_parse_jsonb(ai.get('text_faq')) or [])}, "
        f"highlights_count={len(_parse_jsonb(ai.get('text_highlights')) or [])}, "
        f"hooks_count={len(_parse_jsonb(ai.get('marketing_hooks')) or [])}, "
        f"primary_domain={ctx.get('_primary_domain')}, "
        f"rating={ctx.get('rating_value')}/{ctx.get('review_count')}"
    )

    client = OpenAI(api_key=OPENAI_API_KEY)
    script = generate_script(client, ctx)
    dialogue = script.get("dialogue") or ""
    if not dialogue.strip():
        logging.error("[PODCAST] script vuoto")
        sys.exit(2)

    logging.info("[PODCAST] dialogue script (display/transcript):\n" + dialogue)

    # Preprocessing per TTS: converti il dominio in forma phonetic-friendly
    tts_dialogue = _preprocess_for_tts(dialogue, ctx.get("_primary_domain"))
    if tts_dialogue != dialogue:
        logging.info("[PODCAST] domain preprocessed for TTS readability")

    pcm = synthesize_with_gemini(tts_dialogue)
    wav_bytes = pcm_to_wav(pcm)

    out_dir = Path("/tmp")
    out_dir.mkdir(parents=True, exist_ok=True)
    wav_path = out_dir / f"podcast_demo_{vehicle_id[:8]}.wav"
    mp3_path = out_dir / f"podcast_demo_{vehicle_id[:8]}.mp3"

    wav_path.write_bytes(wav_bytes)
    logging.info(f"[PODCAST] wav saved: {wav_path} ({len(wav_bytes)} bytes)")

    try:
        wav_to_mp3(wav_path, mp3_path)
        logging.info(f"[PODCAST] mp3 saved: {mp3_path} ({mp3_path.stat().st_size} bytes)")
    except subprocess.CalledProcessError as e:
        logging.error(f"[PODCAST] ffmpeg failed: {e.stderr.decode()[:300]}")

    # Print metadata summary
    print("\n" + "=" * 60)
    print("PODCAST DEMO COMPLETE")
    print("=" * 60)
    print(f"Title:       {script.get('title')}")
    print(f"Description: {script.get('description')}")
    print(f"Est duration: {script.get('duration_estimate_sec')}s")
    print(f"WAV:         {wav_path}")
    print(f"MP3:         {mp3_path}")
    print("=" * 60)
    print("\nApri il file MP3 con un player audio per ascoltare il risultato.")


if __name__ == "__main__":
    main()
