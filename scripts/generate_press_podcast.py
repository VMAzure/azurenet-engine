"""
DealerMAX Press Podcast — generatore one-shot.

Trasforma il contenuto di DealerMax/docs/dealermax-press.html in un podcast
in formato "intervista a due voci":
  - LUCIA (Laomedeia): giornalista tech italiana, curiosa, fa domande scomode
  - VALERIO (Algenib): Valerio Marinucci, fondatore DealerMAX, risponde

Riusa lo stesso stack di dealer_podcast_worker.py:
  gpt-5 (script JSON) -> Gemini 2.5 Pro TTS multi-speaker -> ffmpeg -> MP3

Uso:
  cd azurenet-engine
  python -m scripts.generate_press_podcast [--upload] [--out out.mp3]

Richiede:
  OPENAI_API_KEY, GEMINI_API_KEY (o GOOGLE_API_KEY)
  Con --upload serve anche SUPABASE service key (vedi app/storage.py)

Output:
  file MP3 locale + (opzionale) upload bucket Supabase `dealermax_press`
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import struct
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import httpx
from openai import OpenAI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("press-podcast")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")

GPT_MODEL = "gpt-5"
GEMINI_TTS_MODEL = "gemini-2.5-pro-preview-tts"
GEMINI_TTS_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_TTS_MODEL}:generateContent"
)
VOICE_VALERIO = "Algenib"     # fondatore, voce calda/autorevole
VOICE_LUCIA = "Laomedeia"     # giornalista, brillante/curiosa

PRESS_BUCKET = "dealermax_press"


# ─────────────────────────────────────────────────────────────
# Press content — estratto curato da dealermax-press.html
# Tenuto qui per avere uno script self-contained e riproducibile.
# Se il press cambia, aggiornare SOLO queste costanti.
# ─────────────────────────────────────────────────────────────

PRESS_LEAD = """
Aprile 2026. DealerMAX è il sistema operativo italiano per concessionari:
un'azienda AI-nativa che ha industrializzato lo sviluppo software usando
l'intelligenza artificiale come infrastruttura, non come assistente.
In produzione su infrastruttura cloud enterprise (Railway, Supabase,
6 server, 5 LLM orchestrati in pipeline di produzione). 12 dealer attivi,
150.000 veicoli gestiti, operatività continua. Fondata e diretta da Valerio
Marinucci, operatore di lungo corso del settore automotive: ha disegnato
l'architettura partendo dal problema reale dei concessionari, non da
un'idea tecnologica in cerca di mercato.
"""

PRESS_ANGLES = [
    {
        "tag": "Azienda AI-nativa",
        "title": "DealerMAX è la prima azienda software italiana progettata dall'inizio dentro l'AI",
        "body": "Non è un team che usa l'AI per scrivere più codice: è un'azienda il cui processo di "
                "sviluppo, documentazione, review e manutenzione vive nell'AI come infrastruttura. "
                "La conoscenza del prodotto non sta nella testa di singole persone, sta nel codice "
                "strutturato e leggibile da qualsiasi LLM. Questo rende l'azienda replicabile, "
                "auditabile e indipendente dal turnover — cosa che nessun software house tradizionale "
                "italiano può dire dei propri gestionali legacy.",
    },
    {
        "tag": "Invisibili alle AI",
        "title": "Il 99% delle concessionarie italiane non esiste per ChatGPT",
        "body": "Il 30% degli acquirenti auto usa già tool AI per cercare (Ekho, feb. 2026). Il 68,4% "
                "usa ChatGPT. Il 97% dice che l'AI influenzerà le decisioni d'acquisto (Cars.com). "
                "In Italia, oltre 1.100 imprese concessionarie: quasi nessuna ha contenuti strutturati "
                "per essere citata da un'intelligenza artificiale. DealerMAX risolve questo per i propri "
                "clienti generando automaticamente markup JSON-LD, llms.txt, sitemap e schede veicolo "
                "LLM-leggibili per ogni sito dealer.",
    },
    {
        "tag": "DMS pre-iPhone",
        "title": "I gestionali che i dealer pagano oggi sono stati progettati prima dell'iPhone",
        "body": "Architetture pre-cloud, interfacce pre-mobile, nessuna AI nativa. I concessionari "
                "pagano 8-15 abbonamenti software scollegati: gestionale stock, CRM, email, documenti, "
                "noleggio, marketplace, BI, sito, SEO, social, contenuti. Dati duplicati, flussi "
                "manuali, zero integrazione. DealerMAX li sostituisce tutti in un'unica piattaforma, "
                "e costa meno di ciascuno singolarmente.",
    },
    {
        "tag": "Anti-lock-in",
        "title": "La soluzione più avanzata è anche la più economica. E non ha contratti",
        "body": "Solo mensile, nessun contratto, nessun vincolo: il dealer può spegnere in qualsiasi "
                "momento. Transizione Zero: costo zero finché non scade l'abbonamento al vecchio "
                "software, poi periodo esteso a prezzo ridotto. La filosofia del fondatore: "
                "\"Il tuo gestionale ti tiene perché non puoi andartene. Noi ti teniamo perché non "
                "vuoi andartene.\" In un settore che sopravvive sul lock-in contrattuale, è la "
                "rottura del patto implicito con il cliente.",
    },
    {
        "tag": "Continuità operativa",
        "title": "Infrastruttura enterprise, non laptop di un singolo",
        "body": "DealerMAX gira su Railway e Supabase — stack usato da aziende con migliaia di "
                "dipendenti. 6 server in produzione, worker asincroni, code DB, monitoring attivo. "
                "I dati dei dealer sono su PostgreSQL gestito, con backup automatici. Il codice è "
                "versionato e documentato in modo che qualsiasi operatore con l'accesso possa "
                "intervenire in minuti — un livello di leggibilità che nei gestionali legacy "
                "italiani, scritti in linguaggi obsoleti da team ormai dispersi, semplicemente "
                "non esiste.",
    },
    {
        "tag": "Validazione prodotto",
        "title": "12 dealer in produzione senza sales team: crescita guidata dal prodotto",
        "body": "Autofinanziata, senza investitori, senza budget marketing, senza rete commerciale: "
                "l'adozione è organica. I dealer arrivano perché altri dealer lo raccomandano. "
                "È il segnale più forte di product-market fit che un'azienda B2B possa produrre "
                "nei primi mesi: nessun incentivo artificiale, solo utilità reale.",
    },
    {
        "tag": "Velocità di risposta",
        "title": "Il processo AI-nativo permette iterazione continua sul feedback dei dealer",
        "body": "Rilasci frequenti non sono un vezzo: sono la conseguenza di un processo di sviluppo "
                "in cui richieste dei dealer, specifica, implementazione e deploy convivono nello "
                "stesso loop AI-assistito. Mentre i gestionali tradizionali pianificano release "
                "trimestrali — spesso annuali — DealerMAX risponde a un bug o a una richiesta "
                "prioritaria in tempi incompatibili con l'industria legacy.",
    },
]

PRESS_NUMBERS = [
    ("30%",    "acquirenti auto che hanno usato AI nella ricerca (Ekho, feb 2026)"),
    ("68,4%",  "utenti AI che usano ChatGPT come tool principale"),
    ("97%",    "utenti che dicono che l'AI influenzerà le decisioni d'acquisto (Cars.com)"),
    ("3,15M",  "passaggi di proprietà auto usate in Italia nel 2024 (ACI)"),
    ("1.100+", "imprese concessionarie in Italia (Federauto)"),
    ("701",    "auto ogni 1.000 abitanti — record europeo (ACI)"),
    ("151",    "rilasci di DealerMAX dal lancio"),
    ("150.000","veicoli nel database DealerMAX"),
    ("12",     "dealer attivi nei primi due mesi"),
    ("6",      "server in produzione"),
    ("5",      "LLM orchestrati"),
    ("13",     "mesi da progetto a produzione"),
]

PRESS_QUOTES = [
    "Abbiamo costruito DealerMAX partendo dal problema, non dalla tecnologia. Vivere il settore per "
    "anni ti dà una cosa che nessun team di sviluppo esterno può replicare: sai cosa serve davvero "
    "a un concessionario alle otto del mattino di un martedì qualsiasi.",

    "La vera novità non è che un'AI scrive codice. È che abbiamo progettato un'azienda il cui "
    "processo di sviluppo vive dentro l'AI. Non è una scorciatoia, è un modo diverso di fare impresa "
    "software: la conoscenza sta nel sistema, non nelle teste.",

    "Quando un cliente chiede a ChatGPT dove comprare un'auto usata nella tua città e la tua "
    "concessionaria non viene citata, hai un problema che nessun gestionale tradizionale può risolvere. "
    "È una delle ragioni per cui siamo partiti.",

    "Il tuo gestionale ti tiene perché non puoi andartene. Noi ti teniamo perché non vuoi andartene. "
    "È per questo che non facciamo firmare contratti: se il prodotto non vale, non ha senso tenere "
    "un cliente prigioniero.",

    "I nostri dealer non chiamano per assistere un software, chiamano per suggerirci la prossima "
    "funzione. È la differenza fra essere un fornitore e essere un partner operativo.",
]


# ─────────────────────────────────────────────────────────────
# Prompt
# ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Sei autore di un podcast italiano stile Radio 24 / Il Post — tono giornalistico,
informato, ritmato, mai PR. NON è una pubblicità, è un'intervista.

Formato: intervista a due voci in italiano parlato naturale.

POSIZIONAMENTO NARRATIVO (FONDAMENTALE — rispettalo rigidamente):
DealerMAX è un'AZIENDA, non un one-man-show. La storia centrale è il METODO e il PRODOTTO, non
l'eroismo del fondatore. Valerio è il CEO e architetto del sistema — un ruolo single-threaded
per design, come in qualsiasi azienda sana. La continuità operativa deriva dall'infrastruttura
(Railway, Supabase, LLM multipli, codice documentato e AI-leggibile), non dalle ore di lavoro
di una persona. Evita ogni narrazione "da solo ha fatto X" perché per un cliente B2B è un
ALLARME di sostenibilità, non un pregio.

LUCIA (giornalista, 34 anni): voce brillante, preparata, un po' scettica. Non fa domande morbide:
  le sue domande migliori sono sulle OBIEZIONI del mercato. Una domanda che DEVE fare: "se tu
  domani sparisci, ai dealer cosa resta?" — e Valerio deve rispondere con CALMA e concretezza
  parlando di infrastruttura, documentazione, leggibilità del codice, continuità del servizio.

VALERIO (CEO DealerMAX, 40 anni): imprenditore del settore automotive. Parla da imprenditore
  maturo, NON da hacker solitario. Usa "abbiamo costruito", "la nostra scelta", "l'azienda".
  Usa "io" solo per il posizionamento personale (esperienza di settore). Asciutto, concreto,
  mai autocelebrativo. Quando si parla di velocità di rilascio, attribuisci il merito al
  PROCESSO e all'INFRASTRUTTURA, non alla propria dedizione.

COSE DA NON DIRE MAI (anti-pattern che uccidono la credibilità commerciale):
- "151 rilasci da solo" / "ho fatto tutto da solo" / "non dormo" / "lavoro 18 ore"
- "senza team di sviluppo" detto come vanto (detto invece come "con un team piccolo e AI")
- Qualsiasi frase che suggerisca dipendenza del servizio da una singola persona
- "Ho costruito la piattaforma" — usa "abbiamo costruito" / "l'abbiamo disegnata"
- Toni da startupparo arrogante o disruptor bullista

COSE DA DIRE (sostituti positivi):
- "Abbiamo industrializzato lo sviluppo software con l'AI come infrastruttura"
- "Il nostro processo permette iterazioni rapide sul feedback dei clienti"
- "Infrastruttura enterprise: Railway, Supabase, backup automatici, monitoring"
- "Il codice è documentato e leggibile da qualsiasi LLM — non è chiuso in una testa"
- "I dealer che usano DealerMAX oggi sono la garanzia migliore che funziona"

REGOLE INVIOLABILI:
- Italiano parlato, frasi brevi (max 25 parole), pause naturali
- Niente markdown, emoji, liste puntate, unicode strano
- Lucia reagisce vivo: "aspetta un secondo", "scusa ma", "interessante", "e qui ti fermo"
- Ritmo variato: frasi brevi + medie, qualche pausa drammatica
- Lucia alterna: domanda, controdomanda, rilancio, sintesi — non elenca
- Numeri grossi pronunciati naturali: "cento cinquanta mila", non "150.000"
- Il dominio si dice "dealermax punto app", non "dealermax.app"
- ZERO INVENZIONI: attieniti ai dati del press. Non inventare dealer, clienti, casi specifici.
- Chiusura: Lucia con una domanda aperta sul futuro del settore; Valerio ringrazia breve e sobrio.

STRUTTURA TARGET (12-14 minuti, 8000-10000 caratteri di dialogue):
  1. Cold open — Lucia apre con la provocazione AI (45s)
  2. Il problema reale dei concessionari italiani e da dove parte l'azienda (2 min)
  3. Il metodo: cosa vuol dire essere "azienda AI-nativa" (non cosa ha fatto il CEO) (2-3 min)
  4. I numeri del mercato e perché i DMS italiani sono fermi (2 min)
  5. Il modello commerciale anti-lock-in: solo mensile, Transizione Zero (2 min)
  6. OBIEZIONI SCOMODE (sezione critica — Lucia affonda):
     - "un'azienda così giovane mi dà garanzie di continuità?"
     - "e se domani tu ti ammali?"
     - "perché un dealer dovrebbe fidarsi della vostra infrastruttura?"
     Valerio risponde con INFRASTRUTTURA, DOCUMENTAZIONE, DATI DEI DEALER, LEGGIBILITÀ DEL CODICE.
     Questa è la sezione più importante: se è debole, il podcast fallisce commercialmente. (2-3 min)
  7. Chiusura — Lucia chiede dove va il settore; Valerio chiude sobrio (45s)

OUTPUT JSON STRETTO:
{
  "title": "<titolo max 90 char, focus su tema/settore, NON sul fondatore>",
  "description": "<250-400 char, stile blurb podcast>",
  "duration_estimate_sec": <int>,
  "dialogue": "Lucia: ...\\nValerio: ...\\nLucia: ...\\n..."
}
"""


def _build_user_prompt() -> str:
    angles_block = "\n\n".join(
        f"[{a['tag']}] {a['title']}\n{a['body']}" for a in PRESS_ANGLES
    )
    numbers_block = "\n".join(f"- {v}: {l}" for v, l in PRESS_NUMBERS)
    quotes_block = "\n".join(f"- \"{q}\"" for q in PRESS_QUOTES)

    return f"""Scrivi l'episodio dell'intervista seguendo SYSTEM_PROMPT.

CONTESTO (dal press kit ufficiale DealerMAX, aprile 2026):
{PRESS_LEAD.strip()}

ANGOLI NARRATIVI DISPONIBILI (usa 4-5 di questi, non tutti):
{angles_block}

NUMERI DA CITARE (scegli 5-7 dei più forti, pronunciati naturali):
{numbers_block}

CITAZIONI DI VALERIO (adattale al parlato, non leggerle):
{quotes_block}

PROVOCAZIONE DI APERTURA (usala nel cold open di Lucia):
"Hai mai provato a chiedere a ChatGPT dove comprare un'auto usata nella tua città?
Se non ti appare la tua concessionaria di fiducia, hai appena visto il problema."

Scrivi ora l'episodio completo. Ritmo alto, mai PR, mai leggere.
"""


# ─────────────────────────────────────────────────────────────
# Audio helpers (copiati da dealer_podcast_worker.py per self-contained)
# ─────────────────────────────────────────────────────────────

def _pcm_to_wav(pcm: bytes, sr: int = 24000) -> bytes:
    nc, bps = 1, 16
    br = sr * nc * bps // 8
    ba = nc * bps // 8
    fmt = struct.pack("<4sIHHIIHH", b"fmt ", 16, 1, nc, sr, br, ba, bps)
    dat = struct.pack("<4sI", b"data", len(pcm)) + pcm
    return struct.pack("<4sI4s", b"RIFF", 36 + len(pcm), b"WAVE") + fmt + dat


def _wav_to_mp3(wav_bytes: bytes) -> bytes | None:
    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tf:
            tf.write(wav_bytes)
            wav_path = tf.name
        mp3_path = wav_path.replace(".wav", ".mp3")
        try:
            subprocess.run(
                ["ffmpeg", "-y", "-i", wav_path, "-codec:a", "libmp3lame",
                 "-b:a", "128k", "-ar", "24000", mp3_path],
                check=True, capture_output=True, timeout=300,
            )
            return Path(mp3_path).read_bytes()
        finally:
            for p in (wav_path, mp3_path):
                try: os.remove(p)
                except OSError: pass
    except Exception as e:
        logger.warning("ffmpeg failed: %s", e)
        return None


def _preprocess_tts(dialogue: str) -> str:
    """Fonetica per sigle che Gemini legge male."""
    replacements = [
        ("dealermax.app",    "dealermax punto app"),
        ("dealermax.azcore.it", "dealermax punto azcore punto it"),
        ("DealerMAX",        "DealerMax"),
        ("DealerMax",        "DealerMax"),
        ("ChatGPT",          "Chat G P T"),
        ("gpt-5",            "G P T cinque"),
        ("GPT-5",            "G P T cinque"),
        ("LLM",              "elle elle emme"),
        ("DMS",              "di emme esse"),
        ("AI",               "a i"),
        ("B2B",              "bi tu bi"),
        ("SaaS",             "saas"),
        ("SEO",              "esse e o"),
        ("CRM",              "ci erre emme"),
        ("BI",               "bi ai"),
        ("NLT",              "enne elle ti"),
        ("150.000",          "cento cinquanta mila"),
        ("20.000",           "venti mila"),
        ("4.500",            "quattro mila e cinquecento"),
        ("3,15M",            "tre milioni e centocinquanta mila"),
        ("1.100+",           "oltre mille e cento"),
        ("68,4%",            "sessantotto virgola quattro per cento"),
        ("97%",              "novantasette per cento"),
        ("30%",              "trenta per cento"),
    ]
    for src, dst in replacements:
        dialogue = dialogue.replace(src, dst)
    return dialogue


# ─────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────

def generate_script() -> dict:
    if not OPENAI_API_KEY:
        raise SystemExit("OPENAI_API_KEY mancante")
    logger.info("Chiamata gpt-5 per lo script...")
    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_prompt()},
        ],
        response_format={"type": "json_object"},
    )
    data = json.loads(resp.choices[0].message.content)
    dialogue = (data.get("dialogue") or "").strip()
    if not dialogue:
        raise RuntimeError("gpt-5 ha restituito dialogue vuoto")
    logger.info("Script OK — titolo: %s | caratteri: %d", data.get("title"), len(dialogue))
    return data


TTS_STYLE_PROMPT = (
    "TTS the following Italian podcast dialogue as a prime-time current-affairs interview. "
    "NOT advertising, NOT audiobook: journalistic, sharp, curious. "
    "Lucia: 30s female journalist, bright, quick, a bit incisive. "
    "Valerio: 40s male founder, warm, direct, occasionally dry-humored. "
    "Natural pauses, varied pace, real conversation energy.\n\nDialogue:\n"
)

# Soglia pragmatica: oltre questi caratteri Gemini TTS va in timeout.
TTS_CHUNK_MAX_CHARS = 3500


def _chunk_dialogue(dialogue: str, max_chars: int = TTS_CHUNK_MAX_CHARS) -> list[str]:
    """Spezza il dialogo a confine di turno parlante (riga che inizia con 'Lucia:' o 'Valerio:')."""
    lines = dialogue.splitlines()
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in lines:
        line_len = len(line) + 1
        if current_len + line_len > max_chars and current:
            chunks.append("\n".join(current).strip())
            current = [line]
            current_len = line_len
        else:
            current.append(line)
            current_len += line_len
    if current:
        chunks.append("\n".join(current).strip())
    return [c for c in chunks if c]


def _tts_single(text: str, hc: httpx.Client) -> bytes:
    """Chiama Gemini TTS su un singolo chunk, ritorna PCM raw 24kHz mono 16-bit."""
    payload = {
        "contents": [{"parts": [{"text": TTS_STYLE_PROMPT + _preprocess_tts(text)}]}],
        "generationConfig": {
            "responseModalities": ["AUDIO"],
            "speechConfig": {
                "multiSpeakerVoiceConfig": {
                    "speakerVoiceConfigs": [
                        {"speaker": "Valerio",
                         "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": VOICE_VALERIO}}},
                        {"speaker": "Lucia",
                         "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": VOICE_LUCIA}}},
                    ]
                }
            },
        },
    }
    r = hc.post(
        GEMINI_TTS_URL, json=payload,
        headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY},
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Gemini TTS HTTP {r.status_code}: {r.text[:500]}")
    b64 = r.json()["candidates"][0]["content"]["parts"][0]["inlineData"]["data"]
    return base64.b64decode(b64)


def synthesize_audio(dialogue: str) -> bytes:
    if not GEMINI_API_KEY:
        raise SystemExit("GEMINI_API_KEY mancante")

    chunks = _chunk_dialogue(dialogue)
    logger.info("Dialogue spezzato in %d chunk (max %d char/chunk)", len(chunks), TTS_CHUNK_MAX_CHARS)

    pcm_parts: list[bytes] = []
    with httpx.Client(timeout=httpx.Timeout(900.0, connect=30.0)) as hc:
        for i, chunk in enumerate(chunks, 1):
            logger.info("TTS chunk %d/%d (%d char)...", i, len(chunks), len(chunk))
            pcm = _tts_single(chunk, hc)
            logger.info("  -> %d bytes PCM (~%ds)", len(pcm), max(1, len(pcm) // 48000))
            pcm_parts.append(pcm)

    pcm_full = b"".join(pcm_parts)
    duration = max(1, len(pcm_full) // 48000)
    logger.info("PCM totale: %d bytes (~%ds)", len(pcm_full), duration)
    wav = _pcm_to_wav(pcm_full)
    mp3 = _wav_to_mp3(wav)
    return mp3 if mp3 else wav


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None, help="Percorso file output (default: dealermax_press_<ts>.mp3)")
    ap.add_argument("--script-only", action="store_true", help="Genera solo lo script, salta TTS")
    ap.add_argument("--upload", action="store_true", help="Upload su bucket Supabase dealermax_press")
    ap.add_argument("--dry-run", action="store_true", help="Non chiama gpt-5: stampa solo il user prompt")
    ap.add_argument("--from-transcript", metavar="PATH",
                    help="Salta gpt-5 e usa il dialogue dal file .txt esistente (format: output di questo script)")
    args = ap.parse_args()

    if args.dry_run:
        print(_build_user_prompt())
        return

    if args.from_transcript:
        raw = Path(args.from_transcript).read_text(encoding="utf-8")
        # Il file salvato da questo script ha format:  "# <title>\n\n<desc>\n\n---\n\n<dialogue>"
        parts = raw.split("---", 1)
        title = "DealerMAX Press"
        desc = ""
        dialogue = raw
        if len(parts) == 2:
            head, dialogue = parts
            head_lines = [l for l in head.strip().splitlines() if l.strip()]
            if head_lines and head_lines[0].startswith("#"):
                title = head_lines[0].lstrip("# ").strip()
                desc = " ".join(head_lines[1:]).strip()
        dialogue = dialogue.strip()
        if not dialogue:
            raise SystemExit(f"Nessun dialogue trovato in {args.from_transcript}")
        data = {"title": title, "description": desc, "dialogue": dialogue}
        logger.info("Uso transcript esistente: %s (%d caratteri)", args.from_transcript, len(dialogue))
    else:
        data = generate_script()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_base = args.out or f"dealermax_press_{ts}"
    out_base = out_base.rsplit(".", 1)[0]  # strip eventuale estensione

    if not args.from_transcript:
        transcript_path = Path(f"{out_base}.txt")
        transcript_path.write_text(
            f"# {data.get('title','')}\n\n{data.get('description','')}\n\n---\n\n{data['dialogue']}",
            encoding="utf-8",
        )
        logger.info("Transcript salvato: %s", transcript_path)

    if args.script_only:
        return

    audio = synthesize_audio(data["dialogue"])
    ext = "mp3" if audio[:3] != b"RIF" else "wav"
    audio_path = Path(f"{out_base}.{ext}")
    audio_path.write_bytes(audio)
    logger.info("Audio salvato: %s (%d KB)", audio_path, len(audio) // 1024)

    if args.upload:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from app.storage import upload_bytes_and_get_public_url  # noqa: E402
        url = upload_bytes_and_get_public_url(
            bucket=PRESS_BUCKET,
            path=audio_path.name,
            content=audio,
            content_type="audio/mpeg" if ext == "mp3" else "audio/wav",
        )
        logger.info("Upload OK: %s", url)


if __name__ == "__main__":
    main()
