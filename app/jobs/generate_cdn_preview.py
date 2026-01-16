import logging
from app.models import (
    MnetModelli,
    MnetMarche,
    AzImage,
    MnetModelliCdnPreview,
)
from urllib.parse import urlencode

from sqlalchemy.orm import Session

from app.database import SessionLocal


IMAGIN_CDN_BASE_URL = "https://cdn.imagin.studio/getImage"


def normalize_model_family(descrizione: str) -> str:
    """
    Normalizzazione MINIMA.
    Le vere eccezioni vivono in:
    - az_image
    - mnet_modelli_correzioni (a monte)
    """
    if not descrizione:
        return ""

    return (
        descrizione
        .strip()
        .lower()
        .replace("  ", " ")
        .replace(" ", "-")
        .strip("-")
    )


def build_cdn_url(make: str, model_family: str) -> str:
    params = {
        "customer": "leasys",
        "make": make,
        "modelFamily": model_family,
        "angle": 23,
        "zoomType": "fullscreen",
        "width": 1200,
    }
    return f"{IMAGIN_CDN_BASE_URL}?{urlencode(params)}"


def generate_cdn_preview():
    logging.info("[CDN][PREVIEW] START")

    db: Session = SessionLocal()

    try:
        # Solo modelli senza default_img
        modelli = (
            db.query(MnetModelli)
            .filter(MnetModelli.default_img.is_(None))
            .all()
        )

        for modello in modelli:
            # Idempotenza: una sola riga per modello
            exists = (
                db.query(MnetModelliCdnPreview)
                .filter(
                    MnetModelliCdnPreview.codice_modello
                    == modello.codice_modello
                )
                .first()
            )
            if exists:
                continue

            # Recupero marca
            marca = (
                db.query(MnetMarche)
                .filter(MnetMarche.acronimo == modello.marca_acronimo)
                .first()
            )
            if not marca:
                logging.warning(
                    "[CDN][PREVIEW] SKIP codice_modello=%s (marca non trovata)",
                    modello.codice_modello,
                )
                continue

            # --------------------------------------
            # STEP 1 — precedenza assoluta az_image
            # --------------------------------------
            az = (
                db.query(AzImage)
                .filter(AzImage.codice_modello == modello.codice_modello)
                .first()
            )

            if az and az.modello_alias:
                make = (
                    az.marca_alias
                    if az.marca_alias
                    else marca.nome.strip().lower().replace(" ", "-")
                )
                model_family = az.modello_alias
                model_variant = az.model_variant
                source = "az_image"
            else:
                # --------------------------------------
                # STEP 2 — fallback normalizzato
                # --------------------------------------
                make = marca.nome.strip().lower().replace(" ", "-")
                model_family = normalize_model_family(modello.descrizione)
                model_variant = None
                source = "normalized"

            if not make or not model_family:
                logging.warning(
                    "[CDN][PREVIEW] SKIP codice_modello=%s (make/model_family vuoti)",
                    modello.codice_modello,
                )
                continue

            url_cdn = build_cdn_url(make, model_family)

            preview = MnetModelliCdnPreview(
                codice_modello=modello.codice_modello,
                make=make,
                model_family=model_family,
                model_variant=model_variant,
                url_cdn=url_cdn,
                source=source,
                is_valid=False,
            )

            db.add(preview)

        db.commit()
        logging.info("[CDN][PREVIEW] DONE")

    except Exception:
        db.rollback()
        logging.exception("[CDN][PREVIEW] FAILED")
        raise

    finally:
        db.close()


if __name__ == "__main__":
    generate_cdn_preview()
