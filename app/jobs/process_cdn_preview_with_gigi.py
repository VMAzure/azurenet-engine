import time
import uuid
import logging
import asyncio
from pathlib import Path

import httpx
import cv2
import numpy as np
from sqlalchemy import text

from app.database import SessionLocal
from app.storage import upload_bytes_and_get_public_url


# ============================================================
# CONFIG
# ============================================================

SYSTEM_USER_ID = 13

PROMPT = """
Fotografia cinematografica realistica in città metropolitana elegante,
ristoranti e movida serale sullo sfondo, alberi verdi e lampioni stile parigi.
Scatto 150mm, luce cinematografica, riflessi realistici.

Auto in primo piano al 100%, Camera 3/4 front view,
same position, same framing and same perspective as the input image
(reference image), no repositioning or rotation.

No rendering, stile fotografico reale.
""".strip()

# directory temporanea (per test locale)
TMP_DIR = Path("/tmp/gigi_sanitized")
TMP_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ============================================================
# CV — SANITIZE LICENSE PLATE (ON ORIGINAL CDN)
# ============================================================

from ultralytics import YOLO
import cv2
import numpy as np

# caricato una sola volta
_lp_model = YOLO("models/best.pt")


def sanitize_plate_on_original(img_bytes: bytes) -> bytes:
    """
    Usa YOLO license-plate per individuare la targa
    e coprirla con bianco puro (#FFFFFF).
    """
    nparr = np.frombuffer(img_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    if img is None:
        raise RuntimeError("Impossibile decodificare immagine")

    h, w, _ = img.shape

    results = _lp_model.predict(
        source=img,
        conf=0.4,
        imgsz=640,
        verbose=False,
    )

    boxes = []

    for r in results:
        if r.boxes is None:
            continue

        for b in r.boxes:
            x1, y1, x2, y2 = map(int, b.xyxy[0])
            conf = float(b.conf[0])
            area = (x2 - x1) * (y2 - y1)

            boxes.append((area, conf, x1, y1, x2, y2))

    if not boxes:
        logging.info("[CV] Nessuna targa rilevata → skip sanitize")
        ok, buf = cv2.imencode(".png", img)
        if not ok:
            raise RuntimeError("Errore encoding PNG")
        return buf.tobytes()


    # usa la box con area maggiore
    _, conf, x1, y1, x2, y2 = max(boxes, key=lambda x: x[0])

    # SANITIZE: bianco puro
    cv2.rectangle(
        img,
        (x1, y1),
        (x2, y2),
        (255, 255, 255),
        thickness=-1,
    )

    ok, buf = cv2.imencode(".png", img)
    if not ok:
        raise RuntimeError("Errore encoding PNG")

    return buf.tobytes()


# ============================================================
# DOWNLOAD CDN IMAGE
# ============================================================

async def download_image(url: str) -> bytes:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.content

# ============================================================
# MAIN PIPELINE (1 IMAGE PER RUN)
# ============================================================

def process_cdn_previews():
    logging.info("[CDN→CV→JOB] START (SEQUENTIAL MODE)")

    db = SessionLocal()

    try:
        rows = db.execute(text("""
            SELECT codice_modello, url_cdn
            FROM public.mnet_modelli_cdn_preview
            WHERE is_valid = false
            ORDER BY codice_modello
        """)).fetchall()

        if not rows:
            logging.info("[CDN→CV→JOB] NOTHING TO DO")
            return

        for codice_modello, url_cdn in rows:
            logging.info("[CDN→CV→JOB] START modello=%s", codice_modello)

            # 1️⃣ DOWNLOAD
            cdn_bytes = asyncio.run(download_image(url_cdn))

            # 2️⃣ SANITIZE (best-effort)
            sanitized_bytes = sanitize_plate_on_original(cdn_bytes)

         
            # 3️⃣ UPLOAD
            storage_path = f"sanitized/{codice_modello}.png"
            sanitized_url = upload_bytes_and_get_public_url(
                bucket="gigi-gorilla",
                path=storage_path,
                content=sanitized_bytes,
                content_type="image/png",
            )

            # 4️⃣ JOB
            job_id = uuid.uuid4()
            db.execute(text("""
                INSERT INTO public.gigi_gorilla_jobs (
                    id,
                    user_id,
                    subject_url,
                    prompt,
                    orientation,
                    aspect_ratio,
                    model_id,
                    status
                )
                VALUES (
                    :id,
                    :user_id,
                    :subject_url,
                    :prompt,
                    '5:4',
                    '5:4',
                    'gemini-3-pro-image-preview',
                    'queued'
                )
            """), {
                "id": job_id,
                "user_id": SYSTEM_USER_ID,
                "subject_url": sanitized_url,
                "prompt": PROMPT,
            })
            db.commit()  # 🔥 rende il job visibile al worker AI

            # 4️⃣bis — WAIT OUTPUT + WRITE default_img (SYNC BOOTSTRAP)
            start = time.time()
            output_url = None

            while time.time() - start < 20 * 60:  # 20 minuti max
                row = db.execute(text("""
                    SELECT public_url
                    FROM public.gigi_gorilla_job_outputs
                    WHERE job_id = :job_id
                      AND status = 'completed'
                      AND public_url IS NOT NULL
                      AND idx = 0
                    LIMIT 1
                """), {"job_id": job_id}).fetchone()

                if row:
                    output_url = row[0]
                    break

                time.sleep(10)  # polling ogni 10s

            if not output_url:
                logging.error(
                    "[CDN→CV→JOB] TIMEOUT modello=%s job=%s",
                    codice_modello,
                    job_id,
                )
                # ❌ NON segniamo is_valid
                # ❌ verrà ripreso al prossimo run
                continue

            # 4️⃣ter — WRITE default_img (DELTA-ONLY)
            updated = db.execute(text("""
                UPDATE public.mnet_modelli
                SET default_img = :img
                WHERE codice_modello = :codice
                  AND default_img IS NULL
            """), {
                "img": output_url,
                "codice": codice_modello,
            }).rowcount

            if updated == 0:
                logging.warning(
                    "[CDN→CV→JOB] SKIP WRITE modello=%s (default_img già presente)",
                    codice_modello,
                )

            # 5️⃣ MARK DONE
            db.execute(text("""
                UPDATE public.mnet_modelli_cdn_preview
                SET
                    is_valid = true,
                    checked_at = now(),
                    checked_by = 'gigi_gorilla'
                WHERE codice_modello = :codice
            """), {
                "codice": codice_modello,
            })

            db.commit()

            logging.info(
                "[CDN→CV→JOB] DONE modello=%s → job queued (%s)",
                codice_modello,
                job_id,
            )

    except Exception:
        db.rollback()
        logging.exception("[CDN→CV→JOB] FAILED")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    process_cdn_previews()
