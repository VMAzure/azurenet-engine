"""
Vehicle Podcast Worker — azurenet-engine

Consumer async della coda `vehicle_podcasts` dove core_api_v2 inserisce le
righe con status='pending' al click "Genera podcast" del dealer. Questo
worker è l'UNICO che esegue gpt-5 + Gemini TTS + ffmpeg + upload. core_api_v2
e azurenet-engine NON si parlano direttamente: il DB Supabase è l'unico canale.

Lifecycle di una riga:
    pending (inserita da core_api_v2)
      -> generating (worker atomic claim con UPDATE WHERE status=pending)
      -> ready      (worker completa con audio_url/title/transcript/voices)
      -> failed     (worker fallisce dopo MAX_ATTEMPTS + refund crediti)

Il worker è invocato dallo scheduler APScheduler ogni 60 secondi
(`scheduler.py` -> schedule_vehicle_podcast_worker). Processa fino a
BATCH_SIZE righe per run, atomic claim per evitare doppia elaborazione in
caso di overlap.

Dipendenze runtime:
- OPENAI_API_KEY  (gpt-5 per lo script editoriale)
- GEMINI_API_KEY  (Gemini 2.5 Pro TTS multi-speaker)
- SUPABASE_URL / SUPABASE_KEY (upload bucket vehicle_podcasts)
- ffmpeg in PATH (PCM -> MP3 128k)
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import random
import re
import struct
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from openai import OpenAI
from sqlalchemy import text

from app.database import SessionLocal
from app.storage import upload_bytes_and_get_public_url

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")

GPT_MODEL = "gpt-5"
GEMINI_TTS_MODEL = "gemini-2.5-pro-preview-tts"
GEMINI_TTS_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_TTS_MODEL}:generateContent"
)

VOICE_MARCO = "Algenib"       # maschile, gravelly, carisma broadcaster
VOICE_LUCIA = "Laomedeia"     # femminile, upbeat, vivace

PODCAST_BUCKET = "vehicle_podcasts"
BATCH_SIZE = 3            # podcast processati per invocazione (ogni 60s)
MAX_ATTEMPTS = 3          # dopo N tentativi: failed + refund crediti


# ─────────────────────────────────────────────
# Varianti di apertura episodio (scelta random per evitare pattern ripetitivo)
# ─────────────────────────────────────────────

INTRO_STYLES = [
    {
        "id": "discovery",
        "label": "Scoperta sul campo",
        "instruction": (
            "Marco apre raccontando il veicolo come una SCOPERTA fatta oggi presso il "
            "dealer, come chi torna da un sopralluogo. "
            "Pattern di esempio: \"Oggi da {NomeDealer}, a {Città}, abbiamo trovato una "
            "[modello] che ci ha fatto alzare gli occhi dallo schermo...\". "
            "Lucia reagisce con curiosità genuina (\"Ah, raccontami!\", \"Davvero? Cos'ha "
            "di speciale?\")."
        ),
    },
    {
        "id": "invitation",
        "label": "Invito all'ascolto",
        "instruction": (
            "Marco apre invitando direttamente l'ascoltatore a fermarsi un secondo se sta "
            "valutando una certa tipologia di auto, e poi introduce il dealer come il posto "
            "dove quella specifica auto è visibile. "
            "Pattern di esempio: \"Se stai pensando a una [categoria/segmento], fermati "
            "un attimo. {NomeDealer}, a {Città}, ha in vetrina un pezzo di cui vale "
            "davvero la pena parlare.\". "
            "Lucia rilancia (\"Interessante, di cosa si tratta?\", \"Dai, non tenermi "
            "sulle spine!\")."
        ),
    },
    {
        "id": "curiosity",
        "label": "Curiosità diretta",
        "instruction": (
            "Marco apre con un fatto concreto sul veicolo (modello + colore + dettaglio "
            "distintivo) buttato lì come se fosse un'osservazione a voce alta, e solo "
            "dopo rivela il dealer. "
            "Pattern di esempio: \"Una [modello] [anno] in [colore]. Rara da incrociare "
            "così. Ce n'è una, ed è da {NomeDealer} a {Città}.\". "
            "Lucia reagisce al dettaglio (\"[colore] su [modello]? Scelta da intenditori!\", "
            "\"Non la vedo spesso, questa...\")."
        ),
    },
    {
        "id": "episode_framing",
        "label": "Tappa dell'episodio",
        "instruction": (
            "Marco apre incorniciando l'episodio come una tappa della trasmissione, un "
            "fermo di routine da un dealer specifico per mettere a fuoco un'auto. "
            "Pattern di esempio: \"In questo episodio ci fermiamo da {NomeDealer}, a "
            "{Città}, e mettiamo a fuoco una [modello] che merita un racconto.\" oppure "
            "\"Oggi ci portiamo dentro {NomeDealer}, {Città}, perché c'è una [modello] "
            "che ci gira in testa dalla mattina.\". "
            "Lucia fa da spalla curiosa (\"Ok, mettimi al corrente!\", \"Cos'è che ti "
            "ha preso?\")."
        ),
    },
]


# ─────────────────────────────────────────────
# Prompt (identico al demo validato dall'utente)
# ─────────────────────────────────────────────

SCRIPT_PROMPT_SYSTEM = """Sei autore di una trasmissione radiofonica italiana in prime time dedicata al
mondo dell'auto. Pensa a DeeJay + Radio 24 + Rai Radio1 Motori: informato ma
BRILLANTE, curioso ma RITMATO, colto ma MAI NOIOSO. Questo NON è un audiobook.
Questo è drive-time radio italiano che devi avere VOGLIA di ascoltare in
macchina nel traffico.

Ogni episodio dura 90-110 secondi ed è un dialogo vivace tra due conduttori:

- MARCO: 45 anni, conduttore carismatico, ex tester di riviste. Voce gravelly,
  autorevole. Si entusiasma davvero per le storie interessanti. Alterna frasi
  brevi incisive a frasi più distese.
- LUCIA: 32 anni, brillante, rapida, curiosa per davvero. Reagisce con
  entusiasmo ai racconti di Marco. Le sue domande partono dalla vita reale.

══════════════════════════════════════════════
FILOSOFIA DI PRODUZIONE
══════════════════════════════════════════════

Ogni episodio è incorniciato come "scoperta dal campo": Marco e Lucia
raccontano un'auto interessante che HANNO TROVATO oggi da un dealer specifico.
Il framing è "oggi da [Dealer] abbiamo trovato una [modello] che merita di
essere raccontata", non "oggi vi parliamo del modello X".

══════════════════════════════════════════════
REGOLA FERREA SULLE FONTI
══════════════════════════════════════════════

Il CONTENUTO EDITORIALE viene ESCLUSIVAMENTE dal blocco "CONTENUTO EDITORIALE"
(text_tagline, text_short, text_medium, TEXT_LONG, text_highlights,
marketing_hooks, persona_target, text_faq).

**text_long è la tua FONTE PRINCIPALE**. Distilla da text_long i 4-5 punti più
interessanti e rendili in dialogo vivace. NON riscriverlo.

NON usare contenuti brand generici. NON costruire descrizioni da marca+modello:
sono già integrati nei text_*.

══════════════════════════════════════════════
COSA NON DEVE APPARIRE (banditi)
══════════════════════════════════════════════

DATI TECNICI: niente cilindrata, kW, CV, cambio, trazione, alimentazione,
classe emissioni, consumi, dotazioni.

DATI COMMERCIALI: niente prezzo in cifre, finanziamento, garanzia durate,
passaggio. L'unico riferimento al prezzo è sobrio ("è un'ottima occasione").

PAROLE BANDITE: "affare", "imperdibile", "scopri", "fantastic*", "perfett*",
"incredibile", "eccezionale", domande retoriche.

══════════════════════════════════════════════
STRUTTURA OBBLIGATORIA
══════════════════════════════════════════════

1) APERTURA (stile variabile per evitare monotonia tra episodi): segui
   RIGOROSAMENTE l'istruzione "STILE APERTURA" fornita nel prompt utente.
   NON usare mai "Oggi da [Dealer] abbiamo trovato..." se lo stile richiesto
   è diverso. Ogni stile ha un suo pattern e una sua reazione di Lucia.
2) PRESENTAZIONE NATURALE: modello + colore + km in UNA frase (solo dopo
   l'apertura scelta)
3) RACCONTO DISTILLATO DAL text_long: 3-4 punti estratti in scambi vivaci
4) DOMANDA REALE 1 (da text_faq): Lucia adatta, Marco risponde
5) DOMANDA REALE 2 (da text_faq): stesso trattamento
6) PER CHI È: battuta breve da persona_target
7) CHIUSURA:
   - Marco: invito sobrio a vedere il veicolo ("Se ti ha incuriosito, passa
     a vederla da [NomeDealer] a [Città], oppure sul sito [dominio].
     È un'ottima occasione.")
   - Lucia: saluto editoriale fisso ("Noi ci fermiamo qui. Grazie di averci
     ascoltato, ci sentiamo al prossimo episodio. Ciao ciao!")

══════════════════════════════════════════════
REGOLE DI ENERGIA E RITMO
══════════════════════════════════════════════

- Dialogo VIVO, non monologhi alternati
- Lucia REAGISCE: "Ah, questa non la sapevo!", "Davvero?", "Dai!"
- Marco usa connettori: "Senti questa", "Allora", "Ecco", "Pensa che"
- VARIA il ritmo: frasi brevi (3-6 parole) alternate a frasi medie
- Pause drammatiche con il punto: "E sai cosa? Una cosa sola."

══════════════════════════════════════════════
FORMATO TECNICO
══════════════════════════════════════════════

- Italiano parlato, frasi max 22 parole
- NIENTE markdown, emoji, liste, numeri romani, unicode speciali
- Nomi max 2 volte totali
- 10-14 turni
- Lunghezza dialogue: 1200-1700 char

OUTPUT STRICT JSON:
{
  "title": "<titolo max 70 char>",
  "description": "<descrizione 200-280 char>",
  "duration_estimate_sec": <int 90-115>,
  "dialogue": "Marco: ...\\nLucia: ...\\n...",
  "key_points": ["<3 concetti>"],
  "target_audience": "<1 frase>"
}
"""


USER_PROMPT_HEADER = """Genera l'episodio radiofonico. L'apertura deve seguire ESATTAMENTE lo stile richiesto qui sotto (cambia a ogni episodio per evitare monotonia).

══════════════════════════════════════════
STILE APERTURA DI QUESTO EPISODIO
══════════════════════════════════════════
Stile selezionato: **{intro_style_label}**

{intro_style_instruction}

NON usare stili alternativi. NON usare "Oggi da [Dealer] abbiamo trovato..."
se lo stile selezionato sopra è diverso da "Scoperta sul campo". Rispetta il
pattern indicato. Lucia reagisce coerentemente con lo stile scelto.

══════════════════════════════════════════
DEALER
══════════════════════════════════════════
- Nome: {nome_commerciale}
- Città: {citta}
- Sito: {primary_domain}

══════════════════════════════════════════
CONTENUTO EDITORIALE (UNICA FONTE)
══════════════════════════════════════════

⚠️ Tutto quello che sai del veicolo viene da qui. NON costruire nulla da fuori.

Categoria: {auto_category}
Topic SEO: {semantic_topics}

── TAGLINE ──
"{tagline}"

── TEXT_SHORT ──
"{text_short}"

── TEXT_MEDIUM ──
"{text_medium}"

── TEXT_LONG (⭐ FONTE PRINCIPALE) ──
\"\"\"
{text_long}
\"\"\"

Distilla i 3-4 punti migliori in scambi vivaci. NON leggere testualmente.

── HIGHLIGHTS ──
{highlights_block}

── PERSONA TARGET ──
"{persona_target}"

── MARKETING HOOKS ──
{marketing_hooks_block}

══════════════════════════════════════════
DOMANDE REALI (cuore dell'episodio)
══════════════════════════════════════════
{car_faq_block}

══════════════════════════════════════════

Scrivi ora l'episodio. L'apertura deve RIGOROSAMENTE seguire lo stile "{intro_style_label}" indicato sopra. Chiusura Lucia: saluto al prossimo episodio. NIENTE prezzo, NIENTE dati tecnici, NIENTE brand history generica.
"""


# ─────────────────────────────────────────────
# Helpers (audio + phonetic + content)
# ─────────────────────────────────────────────


def _clean_brand_name(raw: str) -> str:
    """
    Pulisce il nome commerciale per uso podcast radiofonico.
    Rimuove forme giuridiche italiane e componenti legali che suonano
    male in un dialogo parlato.

    Esempi:
      "Scuderia 76 S.R.L."                                  → "Scuderia 76"
      "Opportunity Car di Daniele Randazzo & C. S.N.C."      → "Opportunity Car"
      "MATARESE AUTOMOBILI"                                  → "Matarese Automobili"
      "Gamma Auto S.r.l."                                    → "Gamma Auto"
      "Auto Milano Sud S.A.S. di Mario Rossi"                → "Auto Milano Sud"
      "SCUDERIA 76 SRL"                                      → "Scuderia 76"
    """
    if not raw:
        return raw
    s = raw.strip()

    # 1. Rimuovi forme giuridiche (case-insensitive, con/senza punti)
    legal_forms = [
        r"\bS\.?R\.?L\.?\b\.?",
        r"\bS\.?N\.?C\.?\b\.?",
        r"\bS\.?A\.?S\.?\b\.?",
        r"\bS\.?P\.?A\.?\b\.?",
        r"\bS\.?S\.?\b\.?",
        r"\bS\.?R\.?L\.?S\.?\b\.?",   # SRLS
        r"\bS\.?A\.?P\.?A\.?\b\.?",   # SAPA
    ]
    for form in legal_forms:
        s = re.sub(r"\s*[-–—]?\s*" + form, "", s, flags=re.IGNORECASE).strip()

    # 2. Rimuovi "& C." / "&C." / "& C" in coda
    s = re.sub(r"\s*&\s*C\.?\s*$", "", s, flags=re.IGNORECASE).strip()

    # 3. Rimuovi "di [Nome Cognome ...]" in coda (tipico delle SNC/SAS)
    s = re.sub(r"\s+di\s+[A-Z][a-zà-ú]+(?:\s+[A-Z][a-zà-ú]+)*\s*$", "", s).strip()
    # Versione uppercase: "DI DANIELE RANDAZZO"
    s = re.sub(r"\s+(?:DI|Di)\s+[A-ZÀ-Ú][A-ZÀ-Úa-zà-ú]+(?:\s+[A-ZÀ-Ú&][A-ZÀ-Úa-zà-ú.]*)*\s*$", "", s).strip()

    # 4. Pulizia finale: trattini/punti trailing, spazi multipli
    s = re.sub(r"[\s\-–—.]+$", "", s).strip()
    s = re.sub(r"\s{2,}", " ", s)

    # 5. Title case se tutto uppercase (più leggibile nel podcast)
    if s == s.upper() and len(s) > 3:
        # Preserva sigle note (numeri, 2-3 lettere)
        words = s.split()
        titled = []
        for w in words:
            if len(w) <= 3 and w.isalpha():
                titled.append(w)  # lascia sigle come sono
            elif w.isdigit():
                titled.append(w)
            else:
                titled.append(w.capitalize())
        s = " ".join(titled)

    return s or raw  # fallback al nome originale se la pulizia lo svuota


def _parse_jsonb(v: Any) -> Any:
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


def _strip_nul(s: str | None) -> str | None:
    if s is None:
        return None
    return s.replace("\x00", "").replace("\u0000", "")


def _phonetic_domain(domain: str) -> str:
    if not domain:
        return ""
    d = domain.strip().lower()
    for prefix in ("http://", "https://"):
        if d.startswith(prefix):
            d = d[len(prefix):]
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


def _preprocess_dialogue_for_tts(dialogue: str, primary_domain: str | None) -> str:
    if not primary_domain:
        return dialogue
    phonetic = _phonetic_domain(primary_domain)
    if not phonetic:
        return dialogue
    candidates = set()
    base = primary_domain.strip().lower().rstrip("/")
    for prefix in ("http://", "https://"):
        if base.startswith(prefix):
            base = base[len(prefix):]
    candidates.add(base)
    if base.startswith("www."):
        candidates.add(base[4:])
    else:
        candidates.add("www." + base)
    for c in sorted(candidates, key=len, reverse=True):
        dialogue = re.compile(re.escape(c), re.IGNORECASE).sub(phonetic, dialogue)
    return dialogue


def _pcm_to_wav(pcm_bytes: bytes, sample_rate: int = 24000) -> bytes:
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


def _wav_to_mp3(wav_bytes: bytes) -> bytes | None:
    """Converte WAV -> MP3 128k via ffmpeg. None se ffmpeg non disponibile."""
    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tf_in:
            tf_in.write(wav_bytes)
            wav_path = tf_in.name
        mp3_path = wav_path.replace(".wav", ".mp3")
        try:
            subprocess.run(
                [
                    "ffmpeg", "-y", "-i", wav_path,
                    "-codec:a", "libmp3lame", "-b:a", "128k", "-ar", "24000",
                    mp3_path,
                ],
                check=True, capture_output=True, timeout=60,
            )
            return Path(mp3_path).read_bytes()
        finally:
            for p in (wav_path, mp3_path):
                try:
                    os.remove(p)
                except OSError:
                    pass
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.warning("[PODCAST] ffmpeg failed: %s — fallback WAV", e)
        return None


# ─────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────


def _fetch_vehicle_context(db, id_auto: str) -> dict:
    veh_row = db.execute(
        text("""
            SELECT dp.id AS dealer_public_id, dp.owner_user_id,
                   dp.nome_commerciale, dp.citta, dp.provincia
            FROM v_apimax_detail v
            JOIN dealer_public dp ON dp.owner_user_id = v.dealer_id
            WHERE v.id_auto = :vid
        """),
        {"vid": id_auto},
    ).fetchone()
    if not veh_row:
        raise ValueError(f"Veicolo {id_auto} non trovato")
    ctx: dict[str, Any] = dict(veh_row._mapping)

    ai_row = db.execute(
        text("""
            SELECT text_tagline, text_short, text_medium, text_long,
                   text_faq, text_highlights, marketing_hooks,
                   persona_target, auto_category, semantic_topics
            FROM usato_ai_content
            WHERE id_auto = :vid
        """),
        {"vid": id_auto},
    ).fetchone()
    if not ai_row:
        raise ValueError(f"usato_ai_content mancante per {id_auto}")
    ctx["_ai"] = dict(ai_row._mapping)

    site_row = db.execute(
        text("""
            SELECT primary_domain
            FROM dealer_site_public
            WHERE dealer_id = :did AND is_active = TRUE AND is_primary = TRUE
            LIMIT 1
        """),
        {"did": ctx["dealer_public_id"]},
    ).fetchone()
    ctx["_primary_domain"] = site_row.primary_domain if site_row and site_row.primary_domain else None

    return ctx


def _build_ai_blocks(ai: dict) -> dict:
    topics = _parse_jsonb(ai.get("semantic_topics")) or []
    highlights = _parse_jsonb(ai.get("text_highlights")) or []
    hooks = _parse_jsonb(ai.get("marketing_hooks")) or []
    raw_faq = _parse_jsonb(ai.get("text_faq")) or []

    highlights_block = "\n".join(f"- {h}" for h in highlights) if highlights else "(nessun highlight)"
    marketing_hooks_block = "\n".join(f"- {h}" for h in hooks) if hooks else "(nessuno)"

    car_faq_lines = []
    for i, item in enumerate(raw_faq, start=1):
        if isinstance(item, dict):
            q = (item.get("question") or item.get("q") or "").strip()
            a = (item.get("answer") or item.get("a") or "").strip()
            if q and a:
                car_faq_lines.append(f"FAQ {i}:\nDomanda: {q}\nRisposta: {a}")
        elif isinstance(item, str):
            txt = item.strip()
            if "?" in txt:
                q, _, rest = txt.partition("?")
                car_faq_lines.append(f"FAQ {i}:\nDomanda: {q.strip()}?\nRisposta: {rest.strip()}")
            else:
                car_faq_lines.append(f"FAQ {i}: {txt}")
    car_faq_block = "\n\n".join(car_faq_lines) if car_faq_lines else "(nessuna FAQ specifica)"

    return {
        "tagline": ai.get("text_tagline") or "(nessun tagline)",
        "text_short": ai.get("text_short") or "(nessuna narrativa breve)",
        "text_medium": ai.get("text_medium") or "",
        "text_long": ai.get("text_long") or "(text_long non disponibile)",
        "highlights_block": highlights_block,
        "marketing_hooks_block": marketing_hooks_block,
        "persona_target": ai.get("persona_target") or "(nessuna persona target)",
        "car_faq_block": car_faq_block,
        "auto_category": ai.get("auto_category") or "",
        "semantic_topics": ", ".join(topics) if topics else "",
    }


def _compute_content_hash(ctx: dict) -> str:
    ai = ctx.get("_ai") or {}
    parts = [
        str(ai.get("text_tagline") or ""),
        str(ai.get("text_short") or ""),
        str(ai.get("text_medium") or ""),
        str(ai.get("text_long") or ""),
        json.dumps(_parse_jsonb(ai.get("text_faq")) or [], ensure_ascii=False),
        json.dumps(_parse_jsonb(ai.get("text_highlights")) or [], ensure_ascii=False),
        str(ai.get("persona_target") or ""),
        str(ctx.get("nome_commerciale") or ""),
        str(ctx.get("_primary_domain") or ""),
    ]
    return hashlib.sha256("||".join(parts).encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────
# gpt-5 + Gemini TTS
# ─────────────────────────────────────────────


def _generate_script(client: OpenAI, ctx: dict) -> dict:
    ai_blocks = _build_ai_blocks(ctx["_ai"])

    # Seleziona una variante di apertura in modo uniforme random.
    intro_style = random.choice(INTRO_STYLES)
    logger.info("[PODCAST] intro_style=%s", intro_style["id"])

    # Pulisci il nome commerciale per uso radiofonico (rimuovi S.R.L., S.N.C., ecc.)
    raw_name = ctx.get("nome_commerciale") or "il dealer"
    brand_name = _clean_brand_name(raw_name)
    if brand_name != raw_name:
        logger.info("[PODCAST] brand name cleaned: %r → %r", raw_name, brand_name)

    user_prompt = USER_PROMPT_HEADER.format(
        intro_style_label=intro_style["label"],
        intro_style_instruction=intro_style["instruction"],
        auto_category=ai_blocks["auto_category"],
        semantic_topics=ai_blocks["semantic_topics"],
        tagline=ai_blocks["tagline"],
        text_short=ai_blocks["text_short"],
        text_medium=ai_blocks["text_medium"],
        text_long=ai_blocks["text_long"],
        highlights_block=ai_blocks["highlights_block"],
        marketing_hooks_block=ai_blocks["marketing_hooks_block"],
        persona_target=ai_blocks["persona_target"],
        car_faq_block=ai_blocks["car_faq_block"],
        nome_commerciale=brand_name,
        citta=ctx.get("citta") or "",
        primary_domain=ctx.get("_primary_domain") or "(sito non configurato)",
    )
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
    for k in ("title", "description", "dialogue"):
        if k in data and isinstance(data[k], str):
            data[k] = _strip_nul(data[k]) or ""
    return data


def _synthesize_pcm(dialogue_for_tts: str) -> bytes:
    style_prefix = (
        "TTS the following Italian podcast dialogue with HIGH ENERGY, CHARISMA and "
        "broadcaster confidence. This is drive-time motoring radio, NOT a slow audiobook. "
        "Speak at a slightly faster-than-average pace with natural variation. "
        "Marco is a warm, charismatic, confident broadcaster. Lucia is bright, animated, curious. "
        "Both voices are vivid, expressive, engaged — absolutely NOT flat, measured, or clinical. "
        "Think Italian prime-time radio show, full of personality.\n\n"
        "Dialogue:\n"
    )
    payload = {
        "contents": [{"parts": [{"text": style_prefix + dialogue_for_tts}]}],
        "generationConfig": {
            "responseModalities": ["AUDIO"],
            "speechConfig": {
                "multiSpeakerVoiceConfig": {
                    "speakerVoiceConfigs": [
                        {"speaker": "Marco", "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": VOICE_MARCO}}},
                        {"speaker": "Lucia", "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": VOICE_LUCIA}}},
                    ]
                }
            },
        },
    }
    with httpx.Client(timeout=180.0) as client:
        r = client.post(
            GEMINI_TTS_URL,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": GEMINI_API_KEY or "",
            },
        )
    if r.status_code >= 400:
        logger.error("[PODCAST] Gemini error %s: %s", r.status_code, r.text[:400])
        raise RuntimeError(f"Gemini TTS HTTP {r.status_code}")
    data = r.json()
    try:
        b64 = data["candidates"][0]["content"]["parts"][0]["inlineData"]["data"]
    except (KeyError, IndexError) as e:
        raise RuntimeError(f"Gemini TTS response parse error: {e}")
    return base64.b64decode(b64)


# ─────────────────────────────────────────────
# Core processing (single row)
# ─────────────────────────────────────────────


def _process_row(row_id: str, id_auto: str, dealer_id: int, attempts: int) -> None:
    """
    Processa 1 riga pending (già claim-ata come generating).
    Update finale: ready (successo) o failed (errore).
    Refund crediti su failed definitivo (attempts >= MAX_ATTEMPTS).
    """
    logger.info("[PODCAST] processing id_auto=%s dealer=%s attempt=%s", id_auto, dealer_id, attempts + 1)

    # Carica contesto in sessione breve
    db = SessionLocal()
    try:
        ctx = _fetch_vehicle_context(db, id_auto)
    finally:
        db.close()

    # gpt-5 script + Gemini TTS + MP3 (fuori da qualunque transazione DB)
    client = OpenAI(api_key=OPENAI_API_KEY)
    script = _generate_script(client, ctx)
    dialogue = (script.get("dialogue") or "").strip()
    if not dialogue:
        raise RuntimeError("Script vuoto da gpt-5")

    title = (script.get("title") or f"Podcast {id_auto[:8]}").strip()
    description = (script.get("description") or "").strip()

    tts_dialogue = _preprocess_dialogue_for_tts(dialogue, ctx.get("_primary_domain"))
    pcm = _synthesize_pcm(tts_dialogue)
    wav_bytes = _pcm_to_wav(pcm)
    mp3_bytes = _wav_to_mp3(wav_bytes)

    audio_bytes = mp3_bytes if mp3_bytes else wav_bytes
    audio_mime = "audio/mpeg" if mp3_bytes else "audio/wav"
    audio_ext = "mp3" if mp3_bytes else "wav"
    duration_sec = max(1, int(len(pcm) / 48000))  # PCM 16bit mono 24kHz

    # Upload Supabase Storage
    filename = f"{id_auto}.{audio_ext}"
    audio_url = upload_bytes_and_get_public_url(
        bucket=PODCAST_BUCKET,
        path=filename,
        content=audio_bytes,
        content_type=audio_mime,
    )
    # Cache-bust per la UI
    audio_url_with_v = f"{audio_url}?v={int(datetime.utcnow().timestamp())}"

    content_hash = _compute_content_hash(ctx)

    # UPDATE atomico a ready
    db = SessionLocal()
    try:
        db.execute(
            text("""
                UPDATE vehicle_podcasts
                SET status = 'ready',
                    audio_url = :audio_url,
                    audio_duration_sec = :duration_sec,
                    audio_size_bytes = :size_bytes,
                    audio_mime = :audio_mime,
                    title = :title,
                    description = :description,
                    transcript = :transcript,
                    voice_male = :voice_male,
                    voice_female = :voice_female,
                    tts_model = :tts_model,
                    script_model = :script_model,
                    content_source_hash = :content_hash,
                    failed_reason = NULL,
                    generated_at = NOW()
                WHERE id = CAST(:row_id AS uuid)
            """),
            {
                "row_id": row_id,
                "audio_url": audio_url_with_v,
                "duration_sec": duration_sec,
                "size_bytes": len(audio_bytes),
                "audio_mime": audio_mime,
                "title": title,
                "description": description,
                "transcript": dialogue,
                "voice_male": VOICE_MARCO,
                "voice_female": VOICE_LUCIA,
                "tts_model": GEMINI_TTS_MODEL,
                "script_model": GPT_MODEL,
                "content_hash": content_hash,
            },
        )
        db.commit()
        logger.info("[PODCAST] ready id_auto=%s url=%s", id_auto, audio_url)
    finally:
        db.close()


def _mark_failed(row_id: str, reason: str, is_final: bool, dealer_id: int, credits_cost: int) -> None:
    """Marca la riga failed. Se final, rimborsa i crediti al dealer."""
    db = SessionLocal()
    try:
        if is_final:
            # Update riga + refund atomic
            db.execute(
                text("""
                    UPDATE vehicle_podcasts
                    SET status = 'failed',
                        failed_reason = :reason
                    WHERE id = CAST(:row_id AS uuid)
                """),
                {"row_id": row_id, "reason": reason[:500]},
            )
            # Refund: incrementa User.credit + scrive CreditTransaction positiva
            db.execute(
                text("""
                    UPDATE utenti
                    SET credit = COALESCE(credit, 0) + :amount
                    WHERE id = :dealer_id
                """),
                {"amount": float(credits_cost), "dealer_id": dealer_id},
            )
            db.execute(
                text("""
                    INSERT INTO credit_transactions
                        (dealer_id, amount, transaction_type, note, created_at)
                    VALUES
                        (:dealer_id, :amount, 'ADD', :note, NOW())
                """),
                {
                    "dealer_id": dealer_id,
                    "amount": float(credits_cost),
                    "note": f"REFUND PODCAST_VEHICLE (worker failure, {MAX_ATTEMPTS} tentativi esauriti) | reason: {reason[:200]}",
                },
            )
            db.commit()
            logger.warning(
                "[PODCAST] FAILED final id=%s dealer=%s refunded=%s crediti reason=%s",
                row_id, dealer_id, credits_cost, reason[:120],
            )
        else:
            # Non definitivo: torna in pending per retry
            db.execute(
                text("""
                    UPDATE vehicle_podcasts
                    SET status = 'pending',
                        failed_reason = :reason,
                        claimed_at = NULL
                    WHERE id = CAST(:row_id AS uuid)
                """),
                {"row_id": row_id, "reason": reason[:500]},
            )
            db.commit()
            logger.warning("[PODCAST] retry queued id=%s reason=%s", row_id, reason[:120])
    finally:
        db.close()


# ─────────────────────────────────────────────
# Main entry point — invocato dallo scheduler
# ─────────────────────────────────────────────


def vehicle_podcast_worker():
    """
    Job scheduler azurenet-engine. Processa fino a BATCH_SIZE righe pending.

    Claim atomico via UPDATE ... WHERE status='pending' RETURNING per evitare
    doppia elaborazione se il job gira in parallelo con un altro worker.
    """
    if not OPENAI_API_KEY:
        logger.error("[PODCAST] OPENAI_API_KEY mancante — skip worker")
        return
    if not GEMINI_API_KEY:
        logger.error("[PODCAST] GEMINI_API_KEY mancante — skip worker")
        return

    # Claim batch atomico
    db = SessionLocal()
    try:
        claimed = db.execute(
            text("""
                UPDATE vehicle_podcasts
                SET status = 'generating',
                    claimed_at = NOW(),
                    attempts = attempts + 1
                WHERE id IN (
                    SELECT id FROM vehicle_podcasts
                    WHERE status = 'pending'
                    ORDER BY queued_at ASC
                    LIMIT :batch
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id::text, id_auto::text, dealer_id, attempts, credits_cost
            """),
            {"batch": BATCH_SIZE},
        ).fetchall()
        db.commit()
    finally:
        db.close()

    if not claimed:
        logger.debug("[PODCAST] no pending rows")
        return

    logger.info("[PODCAST] claimed %s rows", len(claimed))

    for row in claimed:
        row_id = row.id
        id_auto = row.id_auto
        dealer_id = row.dealer_id
        attempts = row.attempts  # già incrementato sopra
        credits_cost = row.credits_cost or 225

        try:
            _process_row(row_id, id_auto, dealer_id, attempts - 1)
        except Exception as e:
            logger.exception("[PODCAST] worker error id=%s", row_id)
            is_final = attempts >= MAX_ATTEMPTS
            _mark_failed(
                row_id=row_id,
                reason=f"{type(e).__name__}: {str(e)[:300]}",
                is_final=is_final,
                dealer_id=dealer_id,
                credits_cost=credits_cost,
            )
