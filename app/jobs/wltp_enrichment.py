import asyncio
import logging
from sqlalchemy import text

from app.database import DBSession
from app.external.motornet import motornet_get

# ============================================================
# CONFIG
# ============================================================

BATCH_SIZE = 20

AUTO_WLTP_URL = (
    "https://webservice.motornet.it/api/v2_0/rest/public/usato/"
    "auto/dettaglio/wltp?codice_motornet={codice}"
)

VCOM_WLTP_URL = (
    "https://webservice.motornet.it/api/v3_0/rest/public/usato/"
    "vcom/dettaglio/wltp?codice_motornet_uni={codice}"
)

logger = logging.getLogger(__name__)

# ============================================================
# HELPERS
# ============================================================

def is_vcom(codice: str) -> bool:
    return codice.startswith("C0")


def build_wltp_url(codice: str) -> str:
    if is_vcom(codice):
        return VCOM_WLTP_URL.format(codice=codice)
    return AUTO_WLTP_URL.format(codice=codice)


# ----------------------------
# Normalizzazione WLTP
# ----------------------------

def normalize_eu_directive(raw: str | None) -> str | None:
    if not raw:
        return None

    v = raw.upper().replace(" ", "").replace("-", "")

    if v.startswith("EURO6DTEMP"):
        return "EURO_6D_TEMP"
    if v.startswith("EURO6DFINAL") or v == "EURO6D":
        return "EURO_6D"
    if v.startswith("EURO6E"):
        return "EURO_6E"
    if v.startswith("EURO6C"):
        return "EURO_6C"
    if v.startswith("EURO6B"):
        return "EURO_6B"
    if v.startswith("EURO6"):
        return "EURO_6"
    if v.startswith("EURO5"):
        return "EURO_5"
    if v.startswith("EURO4"):
        return "EURO_4"
    if v.startswith("EURO3"):
        return "EURO_3"
    if v.startswith("EURO2"):
        return "EURO_2"

    return None


def resolve_directive_from_wltp(
    records: list[dict],
    anno_immatricolazione: int,
) -> str | None:
    """
    Regola congelata:
    - WLTP valido se l'ANNO rientra nella finestra
    """
    for r in records:
        start = r.get("dataInizioValidita")
        end = r.get("dataFineValidita")

        if not start:
            continue

        try:
            start_year = int(start[:4])
            end_year = int(end[:4]) if end else 9999
        except Exception:
            continue

        if start_year <= anno_immatricolazione <= end_year:
            return normalize_eu_directive(r.get("direttivaEuro"))

    return None


# ----------------------------
# Fallback legacy MNET
# ----------------------------

def normalize_legacy_euro(value: str | None) -> str | None:
    if not value:
        return None

    value = value.strip()
    if value in {"2", "3", "4", "5", "6"}:
        return f"EURO_{value}"

    return None


def fetch_legacy_euro(db, codice: str) -> str | None:
    """
    AUTO  -> mnet_dettagli_usato.euro
    VCOM  -> mnet_vcom_dettagli.euro
    """

    if is_vcom(codice):
        row = db.execute(
            text("""
                SELECT euro
                FROM mnet_vcom_dettagli
                WHERE codice_motornet_uni = :codice
                LIMIT 1
            """),
            {"codice": codice},
        ).fetchone()
    else:
        row = db.execute(
            text("""
                SELECT euro
                FROM mnet_dettagli_usato
                WHERE codice_motornet_uni = :codice
                LIMIT 1
            """),
            {"codice": codice},
        ).fetchone()

    if not row:
        return None

    return row[0]

# ============================================================
# WORKER
# ============================================================

def wltp_enrichment_worker():
    logger.info("[WLTP] START")

    with DBSession() as db:
        rows = db.execute(
            text("""
                SELECT
                    id,
                    codice_motornet,
                    anno_immatricolazione
                FROM azlease_usatoauto
                WHERE eu_emission_directive IS NULL
                  AND codice_motornet IS NOT NULL
                  AND anno_immatricolazione IS NOT NULL
                ORDER BY id
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            """),
            {"limit": BATCH_SIZE},
        ).mappings().all()

        if not rows:
            logger.info("[WLTP] NOTHING TO DO")
            return

        for row in rows:
            auto_id = row["id"]
            codice = row["codice_motornet"]
            anno = row["anno_immatricolazione"]

            url = build_wltp_url(codice)
            tipo = "VCOM" if is_vcom(codice) else "AUTO"

            try:
                logger.info("[WLTP] fetch %s (%s)", codice, tipo)

                # --------------------
                # 1) WLTP
                # --------------------
                records = []

                directive = None  # <<<<<< OBBLIGATORIO

                try:
                    data = asyncio.run(motornet_get(url))
                    records = data.get("wltp", [])

                    directive = resolve_directive_from_wltp(records, anno)

                except RuntimeError as e:
                    # WLTP assente (412) → fallback legacy
                    if "PRECONDITION_FAILED" in str(e) or "412" in str(e):
                        logger.info(
                            "[WLTP] %s: nessun record WLTP (412), fallback legacy",
                            codice,
                        )
                    else:
                        raise

                # Fallback legacy (sempre sicuro)
                if not directive:
                    legacy = fetch_legacy_euro(db, codice)
                    directive = normalize_legacy_euro(legacy)

                # Persistenza
                if directive:
                    # caso risolto
                    db.execute(
                        text("""
                            UPDATE azlease_usatoauto
                            SET eu_emission_directive = :directive
                            WHERE id = :id
                        """),
                        {
                            "directive": directive,
                            "id": auto_id,
                        },
                    )
                    logger.info("[WLTP] %s → %s", codice, directive)

                else:
                    # caso IRRECUPERABILE → ND
                    db.execute(
                        text("""
                            UPDATE azlease_usatoauto
                            SET eu_emission_directive = 'ND'
                            WHERE id = :id
                        """),
                        {"id": auto_id},
                    )
                    logger.info("[WLTP] %s → ND (non disponibile)", codice)



            except Exception:
                logger.exception("[WLTP] %s FAILED", codice)

        db.commit()

    logger.info("[WLTP] DONE")
