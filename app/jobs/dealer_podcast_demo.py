"""
Demo: genera podcast dealer presentation (~5 min, 2 voci).
Uso: python app/jobs/dealer_podcast_demo.py [dealer_public_id]
Default: dealer_public_id=48 (Matarese Automobili)
"""
from __future__ import annotations
import base64, json, logging, os, re, struct, subprocess, sys, tempfile
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace") if hasattr(sys.stdout, "reconfigure") else None

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv; load_dotenv()
import httpx
from openai import OpenAI
from sqlalchemy import text
from app.database import SessionLocal

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
if not GEMINI_API_KEY:
    _img_env = _ROOT.parent / "azureimage-engine" / ".env"
    if _img_env.exists():
        for line in _img_env.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("GEMINI_API_KEY="):
                GEMINI_API_KEY = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

DEALER_PUBLIC_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 48

SYSTEM_PROMPT = """Sei autore di una trasmissione radiofonica italiana in prime time dedicata al mondo dell'auto.
Stile DeeJay + Radio 24: informato, BRILLANTE, ritmato, mai noioso.

Episodio speciale: presentiamo una CONCESSIONARIA. Chi sono, cosa fanno, perché fidarsi.

MARCO: 45 anni, conduttore carismatico, ex tester. Voce autorevole ma calda.
LUCIA: 32 anni, brillante, curiosa. Domande dal punto di vista del cliente.

REGOLE INVIOLABILI:
- Italiano parlato naturale, frasi brevi (max 25 parole)
- Niente markdown, emoji, liste, numeri romani, unicode speciali
- Reazioni vive di Lucia: "Ah interessante!", "Davvero?", "Dai!"
- Ritmo variato: frasi brevi + medie, pause drammatiche con il punto
- I due si stuzzicano con affetto, battute leggere
- NIENTE dati tecnici di auto specifiche, NIENTE prezzi
- ZERO HALLUCINATIONS: NON inventare cose non nei dati forniti.
  Non inventare scene ("accoglienza, caffè"), non supporre servizi non elencati,
  non dedurre "team giovane" se non dichiarato. Se un dato manca, non parlarne.
- USA le FAQ fornite: Lucia fa 3-4 domande REALI, Marco risponde con la
  sostanza della risposta FAQ adattata al parlato
- Mantieni RITMO ALTO: sorprendi, incuriosisci, cambia angolo, crea hype
- Se il dealer ha guide, glossario, podcast veicoli: menzionali come segnale
  di autorevolezza editoriale
- Chiusura: Marco invita a visitare + dominio sito + Lucia saluta al prossimo episodio

OUTPUT JSON:
{
  "title": "<titolo max 80 char>",
  "description": "<200-350 char>",
  "duration_estimate_sec": <int>,
  "dialogue": "Marco: ...\\nLucia: ...\\n...",
  "key_points": ["<5-7 concetti>"]
}
"""


def fetch_dealer_context(dealer_public_id: int) -> dict:
    db = SessionLocal()
    try:
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

        faqs = db.execute(text("""
            SELECT question, answer FROM dealer_faq
            WHERE is_active = TRUE ORDER BY sort_order NULLS LAST, id LIMIT 8
        """)).fetchall()
    finally:
        db.close()

    if not row:
        raise RuntimeError(f"Dealer {dealer_public_id} non trovato")

    ctx = dict(row._mapping)
    ctx["_faqs"] = [dict(f._mapping) for f in faqs]
    return ctx


def build_user_prompt(ctx: dict) -> str:
    faq_block = "\n".join(
        f"Q: {f['question']}\nA: {(f['answer'] or '')[:400]}"
        for f in ctx.get("_faqs", [])
    )
    return f"""Genera l'episodio di presentazione per questa concessionaria.

DEALER:
- Nome brand: {ctx.get('brand_name') or ctx.get('nome_commerciale')}
- Città: {ctx.get('citta')}, {ctx.get('provincia')}
- Sito: {ctx.get('primary_domain')}
- Anno fondazione: {ctx.get('founding_year')}
- Slogan: "{ctx.get('organization_slogan') or ''}"
- Rating Google: {ctx.get('rating_value')}/5 su {ctx.get('review_count')} recensioni
- Area servita: {ctx.get('area_served')}
- Veicoli in vetrina: {ctx.get('veicoli_attivi')}

CHI SONO:
\"\"\"{ctx.get('entity_summary') or ''}\"\"\"

POSIZIONAMENTO: {ctx.get('market_positioning') or ''}
COMPETENZE: {ctx.get('organization_expertise') or '(non dichiarate)'}
TARGET: \"\"\"{ctx.get('target_profile') or ''}\"\"\"
SERVIZI: {ctx.get('servizi') or '(nessuno)'}

CONTENUTI EDITORIALI PRODOTTI DAL DEALER (menzionali come authority signal):
- Pagina podcast con episodi dedicati per ogni veicolo in vetrina
- Sezione guide pratiche (acquisto, garanzie, finanziamento, noleggio, elettrico)
- Glossario automotive con oltre 200 termini tecnici e giuridici
- FAQ dedicate raggruppate per tema

DOMANDE FREQUENTI DEI CLIENTI (usa 3-4 nel dialogo, adattate al parlato):
{faq_block}

Scrivi l'episodio. Apertura: Marco introduce la concessionaria come tappa speciale.
Sviluppo: storia, servizi, competenze, FAQ reali, contenuti editoriali come authority.
Chiusura: invito a visitare + dominio sito + saluto Lucia.
Durata target: 4-6 minuti (4000-6000 caratteri di dialogue).
Ritmo ALTO, mai calare. Sorprendi. Crea hype.
"""


def phonetic_domain(domain: str) -> str:
    if not domain:
        return ""
    d = domain.lower()
    for pfx in ("http://", "https://"):
        if d.startswith(pfx):
            d = d[len(pfx):]
    if d.startswith("www."):
        d = d[4:]
    d = d.rstrip("/")
    parts = d.split(".")
    spoken = []
    for p in parts:
        if "-" in p:
            spoken.append(" trattino ".join(p.split("-")))
        else:
            spoken.append(p)
    return " punto ".join(spoken)


def preprocess_tts(dialogue: str, domain: str | None) -> str:
    if not domain:
        return dialogue
    ph = phonetic_domain(domain)
    if not ph:
        return dialogue
    base = domain.lower().rstrip("/")
    for pfx in ("http://", "https://"):
        if base.startswith(pfx):
            base = base[len(pfx):]
    candidates = {base}
    if base.startswith("www."):
        candidates.add(base[4:])
    else:
        candidates.add("www." + base)
    for c in sorted(candidates, key=len, reverse=True):
        dialogue = re.compile(re.escape(c), re.IGNORECASE).sub(ph, dialogue)
    return dialogue


def pcm_to_wav(pcm: bytes, sr: int = 24000) -> bytes:
    nc, bps = 1, 16
    byte_rate = sr * nc * bps // 8
    block_align = nc * bps // 8
    fmt = struct.pack("<4sIHHIIHH", b"fmt ", 16, 1, nc, sr, byte_rate, block_align, bps)
    dat = struct.pack("<4sI", b"data", len(pcm)) + pcm
    return struct.pack("<4sI4s", b"RIFF", 36 + len(pcm), b"WAVE") + fmt + dat


def wav_to_mp3(wav_path: str, mp3_path: str):
    subprocess.run(
        ["ffmpeg", "-y", "-i", wav_path, "-codec:a", "libmp3lame",
         "-b:a", "128k", "-ar", "24000", mp3_path],
        check=True, capture_output=True,
    )


def main():
    logging.info("[DEALER-PODCAST] dealer_public_id=%s", DEALER_PUBLIC_ID)

    ctx = fetch_dealer_context(DEALER_PUBLIC_ID)
    brand = ctx.get("brand_name") or ctx.get("nome_commerciale")
    logging.info("[DEALER-PODCAST] dealer=%s (%s)", brand, ctx.get("citta"))

    # 1. gpt-5 script
    logging.info("[DEALER-PODCAST] gpt-5 generating script...")
    client = OpenAI(api_key=OPENAI_API_KEY)
    resp = client.chat.completions.create(
        model="gpt-5",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(ctx)},
        ],
        response_format={"type": "json_object"},
    )
    data = json.loads(resp.choices[0].message.content)
    dialogue = (data.get("dialogue") or "").strip()
    logging.info("[DEALER-PODCAST] script: %s chars, ~%ss", len(dialogue), data.get("duration_estimate_sec"))

    # 2. Gemini TTS
    tts_dialogue = preprocess_tts(dialogue, ctx.get("primary_domain"))
    logging.info("[DEALER-PODCAST] Gemini 2.5 Pro TTS...")
    style = (
        "TTS the following Italian podcast dialogue with HIGH ENERGY, CHARISMA and "
        "broadcaster confidence. This is drive-time motoring radio, NOT a slow audiobook. "
        "Marco has a warm, gravelly, charismatic broadcaster voice. "
        "Lucia is bright, animated, curious — reactions are alive. "
        "Both vivid, expressive, engaging. Italian prime-time radio.\n\nDialogue:\n"
    )
    payload = {
        "contents": [{"parts": [{"text": style + tts_dialogue}]}],
        "generationConfig": {
            "responseModalities": ["AUDIO"],
            "speechConfig": {
                "multiSpeakerVoiceConfig": {
                    "speakerVoiceConfigs": [
                        {"speaker": "Marco", "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": "Algenib"}}},
                        {"speaker": "Lucia", "voiceConfig": {"prebuiltVoiceConfig": {"voiceName": "Laomedeia"}}},
                    ]
                }
            },
        },
    }
    with httpx.Client(timeout=300.0) as hc:
        r = hc.post(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro-preview-tts:generateContent",
            json=payload,
            headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY},
        )
    if r.status_code >= 400:
        logging.error("[DEALER-PODCAST] Gemini error %s: %s", r.status_code, r.text[:400])
        sys.exit(1)

    b64 = r.json()["candidates"][0]["content"]["parts"][0]["inlineData"]["data"]
    pcm = base64.b64decode(b64)
    duration = len(pcm) / 48000
    logging.info("[DEALER-PODCAST] PCM: %s bytes (%.1fs)", len(pcm), duration)

    # 3. WAV + MP3
    wav_path = f"C:/tmp/dealer_podcast_{DEALER_PUBLIC_ID}.wav"
    mp3_path = f"C:/tmp/dealer_podcast_{DEALER_PUBLIC_ID}.mp3"
    Path(wav_path).write_bytes(pcm_to_wav(pcm))
    wav_to_mp3(wav_path, mp3_path)
    mp3_size = Path(mp3_path).stat().st_size
    logging.info("[DEALER-PODCAST] MP3: %s bytes (%.0f KB)", mp3_size, mp3_size / 1024)

    # Save transcript
    tx_path = f"C:/tmp/dealer_podcast_{DEALER_PUBLIC_ID}_transcript.txt"
    Path(tx_path).write_text(
        f"TITLE: {data.get('title')}\n"
        f"DURATION: {duration:.0f}s ({duration/60:.1f} min)\n"
        f"DESCRIPTION: {data.get('description')}\n\n"
        f"{dialogue}\n\n"
        f"KEY POINTS:\n" + "\n".join(f"  - {kp}" for kp in data.get("key_points", [])),
        encoding="utf-8",
    )

    print()
    print("=" * 60)
    print("DEALER PODCAST COMPLETE")
    print("=" * 60)
    print(f"Title:      {data.get('title')}")
    print(f"Duration:   {duration:.0f}s ({duration/60:.1f} min)")
    print(f"MP3:        {mp3_path} ({mp3_size/1024:.0f} KB)")
    print(f"Transcript: {tx_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
