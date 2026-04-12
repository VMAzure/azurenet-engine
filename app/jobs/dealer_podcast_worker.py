"""
Dealer Podcast Worker — azurenet-engine

Consumer async della coda `dealer_podcast` dove core_api_v2 inserisce le
righe con status='pending' al click "Genera podcast dealer" in /setup.
Gratuito (nessun credito). 1 per dealer, rigenerabile.

Pattern identico a vehicle_podcast_worker.py ma con contesto dealer
(entity_summary, services, expertise, FAQ, rating) invece di veicolo.
"""

from __future__ import annotations

import base64, hashlib, json, logging, os, random, re, struct, subprocess, sys
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

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")

GPT_MODEL = "gpt-5"
GEMINI_TTS_MODEL = "gemini-2.5-pro-preview-tts"
GEMINI_TTS_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_TTS_MODEL}:generateContent"
)
VOICE_MARCO = "Algenib"
VOICE_LUCIA = "Laomedeia"
PODCAST_BUCKET = "vehicle_podcasts"  # riusa lo stesso bucket
MAX_ATTEMPTS = 3


# ─────────────────────────────────────────────
# Prompt
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """Sei autore di una trasmissione radiofonica italiana in prime time dedicata al mondo dell'auto.
Stile DeeJay + Radio 24: informato, BRILLANTE, ritmato, mai noioso.

Episodio speciale: presentiamo una CONCESSIONARIA. Chi sono, cosa fanno, perché fidarsi.

MARCO: 45 anni, conduttore carismatico, ex tester. Voce autorevole ma calda.
LUCIA: 32 anni, brillante, curiosa. Domande dal punto di vista del cliente.

REGOLE INVIOLABILI:
- Italiano parlato naturale, frasi brevi (max 25 parole)
- Niente markdown, emoji, liste, numeri romani, unicode speciali
- Reazioni vive di Lucia
- Ritmo variato: frasi brevi + medie, pause drammatiche
- I due si stuzzicano con affetto, battute leggere
- NIENTE dati tecnici di auto specifiche, NIENTE prezzi
- ZERO HALLUCINATIONS: NON inventare cose non nei dati forniti
- USA le FAQ fornite: Lucia fa 3-4 domande REALI, Marco risponde con la
  sostanza della risposta FAQ adattata al parlato
- Mantieni RITMO ALTO: sorprendi, incuriosisci, cambia angolo, crea hype
- Menziona guide, glossario, podcast veicoli come segnale di autorevolezza
- Chiusura: Marco invita a visitare + dominio sito + Lucia saluta

OUTPUT JSON:
{
  "title": "<titolo max 80 char>",
  "description": "<200-350 char>",
  "duration_estimate_sec": <int>,
  "dialogue": "Marco: ...\\nLucia: ...\\n..."
}
"""

USER_TEMPLATE = """Genera l'episodio di presentazione per questa concessionaria.

DEALER:
- Nome brand: {brand_name}
- Città: {citta}, {provincia}
- Sito: {primary_domain}
- Anno fondazione: {founding_year}
- Slogan: "{slogan}"
- Rating Google: {rating_value}/5 su {review_count} recensioni
- Area servita: {area_served}
- Veicoli in vetrina: {veicoli_attivi}

CHI SONO:
\"\"\"{entity_summary}\"\"\"

POSIZIONAMENTO: {market_positioning}
COMPETENZE: {organization_expertise}
TARGET: \"\"\"{target_profile}\"\"\"
SERVIZI: {servizi}

CONTENUTI EDITORIALI PRODOTTI DAL DEALER (authority signal):
- Pagina podcast con episodi dedicati per ogni veicolo in vetrina
- Sezione guide pratiche (acquisto, garanzie, finanziamento, noleggio, elettrico)
- Glossario automotive con oltre 200 termini tecnici e giuridici
- FAQ dedicate raggruppate per tema

DOMANDE FREQUENTI DEI CLIENTI (usa 3-4 nel dialogo):
{faq_block}

Scrivi l'episodio. Apertura: Marco introduce la concessionaria come tappa speciale.
Sviluppo: storia, servizi, competenze, FAQ reali, contenuti editoriali come authority.
Chiusura: invito a visitare + dominio sito + saluto Lucia.
Durata target: 4-6 minuti (4000-6000 caratteri di dialogue).
Ritmo ALTO, mai calare. Sorprendi. Crea hype.
"""


# ─────────────────────────────────────────────
# Helpers (shared with vehicle_podcast_worker)
# ─────────────────────────────────────────────

def _strip_nul(s):
    return s.replace("\x00", "") if s else s

def _phonetic_domain(domain):
    if not domain: return ""
    d = domain.lower()
    for pfx in ("http://", "https://"):
        if d.startswith(pfx): d = d[len(pfx):]
    if d.startswith("www."): d = d[4:]
    d = d.rstrip("/")
    parts = d.split(".")
    spoken = []
    for p in parts:
        spoken.append(" trattino ".join(p.split("-")) if "-" in p else p)
    return " punto ".join(spoken)

def _preprocess_tts(dialogue, domain):
    if not domain: return dialogue
    ph = _phonetic_domain(domain)
    if not ph: return dialogue
    base = domain.lower().rstrip("/")
    for pfx in ("http://", "https://"):
        if base.startswith(pfx): base = base[len(pfx):]
    candidates = {base}
    candidates.add(base[4:] if base.startswith("www.") else "www." + base)
    for c in sorted(candidates, key=len, reverse=True):
        dialogue = re.compile(re.escape(c), re.IGNORECASE).sub(ph, dialogue)
    return dialogue

def _pcm_to_wav(pcm, sr=24000):
    nc, bps = 1, 16
    br = sr * nc * bps // 8
    ba = nc * bps // 8
    fmt = struct.pack("<4sIHHIIHH", b"fmt ", 16, 1, nc, sr, br, ba, bps)
    dat = struct.pack("<4sI", b"data", len(pcm)) + pcm
    return struct.pack("<4sI4s", b"RIFF", 36 + len(pcm), b"WAVE") + fmt + dat

def _wav_to_mp3(wav_bytes):
    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tf:
            tf.write(wav_bytes); wav_path = tf.name
        mp3_path = wav_path.replace(".wav", ".mp3")
        try:
            subprocess.run(["ffmpeg", "-y", "-i", wav_path, "-codec:a", "libmp3lame",
                            "-b:a", "128k", "-ar", "24000", mp3_path],
                           check=True, capture_output=True, timeout=120)
            return Path(mp3_path).read_bytes()
        finally:
            for p in (wav_path, mp3_path):
                try: os.remove(p)
                except: pass
    except Exception as e:
        logger.warning("[DEALER-PODCAST] ffmpeg failed: %s", e)
        return None

def _clean_brand_name(raw):
    if not raw: return raw
    s = raw.strip()
    for form in [r"\bS\.?R\.?L\.?\b\.?", r"\bS\.?N\.?C\.?\b\.?", r"\bS\.?A\.?S\.?\b\.?",
                 r"\bS\.?P\.?A\.?\b\.?", r"\bS\.?S\.?\b\.?", r"\bS\.?R\.?L\.?S\.?\b\.?"]:
        s = re.sub(r"\s*[-–—]?\s*" + form, "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s*&\s*C\.?\s*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s+di\s+[A-Z][a-zà-ú]+(?:\s+[A-Z][a-zà-ú]+)*\s*$", "", s).strip()
    s = re.sub(r"\s+(?:DI|Di)\s+[A-ZÀ-Ú][A-ZÀ-Úa-zà-ú]+(?:\s+[A-ZÀ-Ú&][A-ZÀ-Úa-zà-ú.]*)*\s*$", "", s).strip()
    s = re.sub(r"[\s\-–—.]+$", "", s).strip()
    s = re.sub(r"\s{2,}", " ", s)
    if s == s.upper() and len(s) > 3:
        words = s.split()
        s = " ".join(w if (len(w) <= 3 and w.isalpha()) or w.isdigit() else w.capitalize() for w in words)
    return s or raw


# ─────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────

def _fetch_dealer_context(db, dealer_public_id):
    row = db.execute(text("""
        SELECT
            c.entity_summary, c.market_positioning, c.organization_slogan,
            c.organization_expertise, c.founding_year, c.primary_vehicle_focus,
            c.target_profile,
            dp.brand_name, dp.nome_commerciale, dp.citta, dp.provincia,
            dp.rating_value, dp.review_count, dp.area_served,
            (SELECT COUNT(*) FROM v_apimax_listing vl
             WHERE vl.dealer_id = dp.owner_user_id AND vl.is_attiva AND vl.visibile) AS veicoli_attivi,
            (SELECT string_agg(sub.title, E'\\n' ORDER BY sub.sort_order)
             FROM (SELECT dss.title, dss.sort_order
                   FROM dealer_site_service_public dss
                   JOIN dealer_site_public dsp ON dsp.id = dss.site_id
                   WHERE dsp.dealer_id = dp.id AND dss.is_visible) sub) AS servizi,
            dsp2.primary_domain
        FROM dealer_site_content_public c
        JOIN dealer_site_public dsp2 ON dsp2.id = c.site_id
        JOIN dealer_public dp ON dp.id = dsp2.dealer_id
        WHERE dp.id = :did LIMIT 1
    """), {"did": dealer_public_id}).fetchone()
    if not row:
        raise ValueError(f"Dealer {dealer_public_id} non trovato")

    faqs = db.execute(text("""
        SELECT question, answer FROM dealer_faq
        WHERE is_active = TRUE ORDER BY sort_order NULLS LAST, id LIMIT 8
    """)).fetchall()

    ctx = dict(row._mapping)
    ctx["_faqs"] = [dict(f._mapping) for f in faqs]
    return ctx


# ─────────────────────────────────────────────
# Generation pipeline
# ─────────────────────────────────────────────

def _process_dealer(dealer_public_id, attempts):
    logger.info("[DEALER-PODCAST] processing dealer_id=%s attempt=%s", dealer_public_id, attempts + 1)

    db = SessionLocal()
    try:
        ctx = _fetch_dealer_context(db, dealer_public_id)
    finally:
        db.close()

    brand = (ctx.get("brand_name") or "").strip()
    if not brand:
        brand = _clean_brand_name(ctx.get("nome_commerciale") or "il dealer")

    faq_block = "\n".join(
        f"Q: {f['question']}\nA: {(f['answer'] or '')[:400]}"
        for f in ctx.get("_faqs", [])
    )

    user_prompt = USER_TEMPLATE.format(
        brand_name=brand,
        citta=ctx.get("citta") or "",
        provincia=ctx.get("provincia") or "",
        primary_domain=ctx.get("primary_domain") or "",
        founding_year=ctx.get("founding_year") or "(non dichiarato)",
        slogan=ctx.get("organization_slogan") or "",
        rating_value=ctx.get("rating_value") or "n/d",
        review_count=ctx.get("review_count") or 0,
        area_served=ctx.get("area_served") or "(non dichiarata)",
        veicoli_attivi=ctx.get("veicoli_attivi") or 0,
        entity_summary=ctx.get("entity_summary") or "(non disponibile)",
        market_positioning=ctx.get("market_positioning") or "",
        organization_expertise=ctx.get("organization_expertise") or "(non dichiarate)",
        target_profile=ctx.get("target_profile") or "(non dichiarato)",
        servizi=ctx.get("servizi") or "(nessuno)",
        faq_block=faq_block,
    )

    # gpt-5
    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
    )
    data = json.loads(resp.choices[0].message.content)
    dialogue = _strip_nul(data.get("dialogue") or "").strip()
    if not dialogue:
        raise RuntimeError("Script vuoto da gpt-5")

    title = _strip_nul(data.get("title") or f"Podcast {brand}").strip()
    description = _strip_nul(data.get("description") or "").strip()

    # TTS
    tts_dialogue = _preprocess_tts(dialogue, ctx.get("primary_domain"))
    style = (
        "TTS the following Italian podcast dialogue with HIGH ENERGY, CHARISMA and "
        "broadcaster confidence. Drive-time motoring radio, NOT audiobook. "
        "Marco: warm, gravelly, charismatic. Lucia: bright, animated, curious. "
        "Both vivid, engaging. Italian prime-time radio.\n\nDialogue:\n"
    )
    payload = {
        "contents": [{"parts": [{"text": style + tts_dialogue}]}],
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
    with httpx.Client(timeout=300.0) as hc:
        r = hc.post(GEMINI_TTS_URL, json=payload,
                     headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY or ""})
    if r.status_code >= 400:
        raise RuntimeError(f"Gemini TTS HTTP {r.status_code}")
    b64 = r.json()["candidates"][0]["content"]["parts"][0]["inlineData"]["data"]
    pcm = base64.b64decode(b64)
    duration_sec = max(1, int(len(pcm) / 48000))

    wav = _pcm_to_wav(pcm)
    mp3 = _wav_to_mp3(wav)
    audio_bytes = mp3 if mp3 else wav
    audio_mime = "audio/mpeg" if mp3 else "audio/wav"
    ext = "mp3" if mp3 else "wav"

    filename = f"dealer_{dealer_public_id}.{ext}"
    audio_url = upload_bytes_and_get_public_url(
        bucket=PODCAST_BUCKET, path=filename, content=audio_bytes, content_type=audio_mime,
    )
    audio_url_v = f"{audio_url}?v={int(datetime.utcnow().timestamp())}"

    # UPDATE ready
    db = SessionLocal()
    try:
        db.execute(text("""
            UPDATE dealer_podcast
            SET status = 'ready',
                audio_url = :url, audio_duration_sec = :dur, audio_size_bytes = :size,
                audio_mime = :mime, title = :title, description = :desc,
                transcript = :transcript, voice_male = :vm, voice_female = :vf,
                tts_model = :tts, script_model = :sm,
                failed_reason = NULL, generated_at = NOW()
            WHERE dealer_id = :did
        """), {
            "url": audio_url_v, "dur": duration_sec, "size": len(audio_bytes),
            "mime": audio_mime, "title": title, "desc": description,
            "transcript": dialogue, "vm": VOICE_MARCO, "vf": VOICE_LUCIA,
            "tts": GEMINI_TTS_MODEL, "sm": GPT_MODEL, "did": dealer_public_id,
        })
        db.commit()
        logger.info("[DEALER-PODCAST] ready dealer=%s dur=%ss", dealer_public_id, duration_sec)
    finally:
        db.close()


def _mark_failed(dealer_id, reason, is_final):
    db = SessionLocal()
    try:
        if is_final:
            db.execute(text("""
                UPDATE dealer_podcast SET status='failed', failed_reason=:r WHERE dealer_id=:d
            """), {"r": reason[:500], "d": dealer_id})
        else:
            db.execute(text("""
                UPDATE dealer_podcast SET status='pending', failed_reason=:r, claimed_at=NULL WHERE dealer_id=:d
            """), {"r": reason[:500], "d": dealer_id})
        db.commit()
    finally:
        db.close()


# ─────────────────────────────────────────────
# Scheduler entry point
# ─────────────────────────────────────────────

def dealer_podcast_worker():
    if not OPENAI_API_KEY or not GEMINI_API_KEY:
        return

    db = SessionLocal()
    try:
        claimed = db.execute(text("""
            UPDATE dealer_podcast
            SET status = 'generating', claimed_at = NOW(), attempts = attempts + 1
            WHERE dealer_id IN (
                SELECT dealer_id FROM dealer_podcast
                WHERE status = 'pending'
                ORDER BY queued_at ASC LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING dealer_id, attempts
        """)).fetchall()
        db.commit()
    finally:
        db.close()

    if not claimed:
        return

    for row in claimed:
        try:
            _process_dealer(row.dealer_id, row.attempts - 1)
        except Exception as e:
            logger.exception("[DEALER-PODCAST] error dealer=%s", row.dealer_id)
            _mark_failed(row.dealer_id, f"{type(e).__name__}: {str(e)[:300]}", row.attempts >= MAX_ATTEMPTS)
