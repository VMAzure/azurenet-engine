"""
One-shot: converte tutti i podcast in formato WAV → MP3.
Scarica il WAV da Supabase, converte con ffmpeg, re-uploada come MP3,
aggiorna la riga vehicle_podcasts.

Uso:
    python app/jobs/convert_wav_podcasts_to_mp3.py

Richiede ffmpeg in PATH.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import httpx
from dotenv import load_dotenv
from sqlalchemy import text

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.database import SessionLocal
from app.storage import upload_bytes_and_get_public_url

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PODCAST_BUCKET = "vehicle_podcasts"


def convert_wav_to_mp3(wav_bytes: bytes) -> bytes:
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tf:
        tf.write(wav_bytes)
        wav_path = tf.name
    mp3_path = wav_path.replace(".wav", ".mp3")
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", wav_path,
             "-codec:a", "libmp3lame", "-b:a", "128k", "-ar", "24000",
             mp3_path],
            check=True, capture_output=True, timeout=60,
        )
        return Path(mp3_path).read_bytes()
    finally:
        for p in (wav_path, mp3_path):
            try:
                os.remove(p)
            except OSError:
                pass


def main():
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT id::text, id_auto::text, audio_url
            FROM vehicle_podcasts
            WHERE status = 'ready'
              AND audio_mime = 'audio/wav'
              AND audio_url LIKE '%.wav%'
        """)).fetchall()
    finally:
        db.close()

    if not rows:
        logging.info("Nessun podcast WAV da convertire.")
        return

    logging.info(f"{len(rows)} podcast WAV da convertire in MP3.")

    for row in rows:
        row_id = row.id
        id_auto = row.id_auto
        wav_url = row.audio_url.split("?")[0]  # rimuovi cache-bust

        logging.info(f"[{id_auto}] download WAV...")
        try:
            with httpx.Client(timeout=60) as client:
                r = client.get(wav_url)
                r.raise_for_status()
                wav_bytes = r.content
        except Exception as e:
            logging.error(f"[{id_auto}] download failed: {e}")
            continue

        logging.info(f"[{id_auto}] converting {len(wav_bytes)} bytes WAV → MP3...")
        try:
            mp3_bytes = convert_wav_to_mp3(wav_bytes)
        except Exception as e:
            logging.error(f"[{id_auto}] ffmpeg failed: {e}")
            continue

        logging.info(f"[{id_auto}] uploading MP3 ({len(mp3_bytes)} bytes)...")
        try:
            mp3_url = upload_bytes_and_get_public_url(
                bucket=PODCAST_BUCKET,
                path=f"{id_auto}.mp3",
                content=mp3_bytes,
                content_type="audio/mpeg",
            )
        except Exception as e:
            logging.error(f"[{id_auto}] upload failed: {e}")
            continue

        db = SessionLocal()
        try:
            db.execute(text("""
                UPDATE vehicle_podcasts
                SET audio_url = :url,
                    audio_mime = 'audio/mpeg',
                    audio_size_bytes = :size
                WHERE id = CAST(:row_id AS uuid)
            """), {
                "url": mp3_url,
                "size": len(mp3_bytes),
                "row_id": row_id,
            })
            db.commit()
            logging.info(f"[{id_auto}] OK: {len(wav_bytes)} WAV → {len(mp3_bytes)} MP3 ({100 - len(mp3_bytes) * 100 // len(wav_bytes)}% ridotto)")
        except Exception as e:
            db.rollback()
            logging.error(f"[{id_auto}] DB update failed: {e}")
        finally:
            db.close()


if __name__ == "__main__":
    main()
