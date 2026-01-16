# process_cdn_modelli_with_gigi.py

"""
⚠️ ONE-SHOT WORKER — MODEL IMAGES REGEN (OLD → AI)
Source: mnet_modelli.default_img_old
HARD GATE on license plates (GDPR)

Rules:
- If plate is NOT detected → SKIP permanently
- NEVER upload original image
- NEVER pass unsanitized image to AI
- NEVER touch mnet_modelli.default_img here
- NO polling
- NO preview table
- ASYNC AI only
"""

import uuid
import logging
import asyncio
from pathlib import Path
from typing import Optional

import httpx
import cv2
import numpy as np
from sqlalchemy import text

from app.database import SessionLocal
from app.storage import upload_bytes_and_get_public_url

from ultralytics import YOLO

# ============================================================
# CONFIG
# ============================================================

SYSTEM_USER_ID = 13
AI_MODEL_ID = "gemini-3-pro-image-preview"
BATCH_LIMIT = 50  # sicurezza: small batches

SANITIZED_BUCKET = "gigi-gorilla"
SANITIZED_PREFIX = "sanitized_old"

YOLO_MODEL_PATH = "models/best.pt"
YOLO_CONFIDENCE = 0.4
YOLO_IMGSZ = 640

PROMPT = """
Fotografia full cinematografica realistica in città metropolitana elegante,
ristoranti e movida serale sullo sfondo, alberi verdi e lampioni stile parigi.
Scatto 150mm, luce cinematografica, riflessi realistici.

Auto in primo piano al 100% /full screen), Camera 3/4 front view,
same position, same framing and same perspective as the input image
(reference image), no repositioning or rotation.

No rendering, stile fotografico reale.
""".strip()

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# ============================================================
# CV — HARD GATE LICENSE PLATE
# ============================================================

class PlateNotDetected(Exception):
    """Raised when no license plate is detected (HARD STOP)."""


_lp_model = YOLO(YOLO_MODEL_PATH)


def detect_and_sanitize_plate(img_bytes: bytes) -> bytes:
    """
    HARD GATE:
    - If plate NOT detected → raise PlateNotDetected
    - If detected → return sanitized image bytes
    """
    nparr = np.frombuffer(img_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    if img is None:
        raise RuntimeError("Unable to decode image")

    results = _lp_model.predict(
        source=img,
        conf=YOLO_CONFIDENCE,
        imgsz=YOLO_IMGSZ,
        verbose=False,
    )

    boxes = []

    for r in results:
        if r.boxes is None:
            continue
        for b in r.boxes:
            x1, y1, x2, y2 = map(int, b.xyxy[0])
            area = (x2 - x1) * (y2 - y1)
            boxes.append((area, x1, y1, x2, y2))

    if not boxes:
        # No real plate detected → image considered SAFE
        return img_bytes


    _, x1, y1, x2, y2 = max(boxes, key=lambda x: x[0])

    # ---- compute contextual fill color (median of surrounding area, excluding plate) ----
    pad = 6  # pixels around the plate
    y1c = max(0, y1 - pad)
    y2c = min(img.shape[0], y2 + pad)
    x1c = max(0, x1 - pad)
    x2c = min(img.shape[1], x2 + pad)

    # copia area estesa
    border = img[y1c:y2c, x1c:x2c].copy()

    # azzera la zona della targa (esclusione)
    border[
        (y1 - y1c):(y2 - y1c),
        (x1 - x1c):(x2 - x1c)
    ] = 0

    # estrai solo pixel non neri
    border_pixels = border.reshape(-1, 3)
    border_pixels = border_pixels[border_pixels.any(axis=1)]

    if len(border_pixels) > 0:
        fill_color = np.median(border_pixels, axis=0).astype(np.uint8)
    else:
        # fallback ultra-raro
        fill_color = np.array([180, 180, 180], dtype=np.uint8)


    # ---- sanitize plate with contextual color ----
    cv2.rectangle(
        img,
        (x1, y1),
        (x2, y2),
        color=tuple(int(c) for c in fill_color),
        thickness=-1,
    )


    ok, buf = cv2.imencode(".png", img)
    if not ok:
        raise RuntimeError("PNG encoding failed")

    return buf.tobytes()

# ============================================================
# HTTP — DOWNLOAD IMAGE
# ============================================================

async def download_image(url: str) -> bytes:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.content

# ============================================================
# MAIN WORKER
# ============================================================

def process_cdn_modelli_with_gigi():
    logging.info("[OLD→AI] START ONE-SHOT WORKER")

    db = SessionLocal()

    try:
        rows = db.execute(text("""
            SELECT m.codice_modello, m.default_img_old
            FROM mnet_modelli m
            LEFT JOIN mnet_modelli_img_old_ai r
              ON r.codice_modello = m.codice_modello
            WHERE m.default_img_old IS NOT NULL
              AND r.codice_modello IS NULL
            ORDER BY m.codice_modello
            LIMIT :limit
        """), {"limit": BATCH_LIMIT}).fetchall()

        if not rows:
            logging.info("[OLD→AI] NOTHING TO DO")
            return

        for codice_modello, img_url in rows:
            logging.info("[OLD→AI] PROCESS modello=%s", codice_modello)

            try:
                # 1) DOWNLOAD ORIGINAL (LEGACY)
                original_bytes = asyncio.run(download_image(img_url))

                # 2) HARD GATE — DETECT & SANITIZE
                sanitized_bytes = detect_and_sanitize_plate(original_bytes)

            except PlateNotDetected:
                logging.warning(
                    "[OLD→AI][SKIP] Plate not detected → modello=%s",
                    codice_modello,
                )
                db.execute(text("""
                    INSERT INTO mnet_modelli_img_old_ai
                        (codice_modello, job_id, status)
                    VALUES
                        (:codice, gen_random_uuid(), 'skipped_plate_not_detected')
                """), {"codice": codice_modello})
                db.commit()
                continue

            except Exception as e:
                logging.exception(
                    "[OLD→AI][ERROR] Preprocess failed modello=%s",
                    codice_modello,
                )
                db.rollback()
                continue

            # 3) UPLOAD SANITIZED IMAGE
            storage_path = f"{SANITIZED_PREFIX}/{codice_modello}.png"
            sanitized_url = upload_bytes_and_get_public_url(
                bucket=SANITIZED_BUCKET,
                path=storage_path,
                content=sanitized_bytes,
                content_type="image/png",
            )

            # 4) CREATE AI JOB
            job_id = uuid.uuid4()
            db.execute(text("""
                INSERT INTO gigi_gorilla_jobs (
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
                    :model_id,
                    'queued'
                )
            """), {
                "id": job_id,
                "user_id": SYSTEM_USER_ID,
                "subject_url": sanitized_url,
                "prompt": PROMPT,
                "model_id": AI_MODEL_ID,
            })

            # 🔥 COMMIT IMMEDIATO: rende il job visibile al worker AI
            db.commit()

            # 5) TRACK STATE
            db.execute(text("""
                INSERT INTO mnet_modelli_img_old_ai
                    (codice_modello, job_id, status)
                VALUES
                    (:codice, :job_id, 'queued')
            """), {
                "codice": codice_modello,
                "job_id": job_id,
            })
            db.commit()

            logging.info(
                "[OLD→AI] JOB CREATED modello=%s job_id=%s",
                codice_modello,
                job_id,
            )

    except Exception:
        db.rollback()
        logging.exception("[OLD→AI] FATAL ERROR")
        raise

    finally:
        db.close()

        logging.info("[OLD→AI] END")


if __name__ == "__main__":
    process_cdn_modelli_with_gigi()
