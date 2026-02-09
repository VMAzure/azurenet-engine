import asyncio
import random

import logging
from datetime import date
from sqlalchemy import text

from app.database import DBSession
from app.external.motornet import motornet_get

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================
# ENDPOINTS — USATO (confermati)
# ============================================================

USATO_MARCHE_URL = "https://webservice.motornet.it/api/v3_0/rest/public/usato/auto/marche"
USATO_MODELLI_URL = "https://webservice.motornet.it/api/v2_0/rest/proxy/usato/auto/modelli"
USATO_VERSIONI_URL = "https://webservice.motornet.it/api/v2_0/rest/proxy/usato/auto/versioni"
USATO_DETTAGLIO_URL = "https://webservice.motornet.it/api/v2_0/rest/public/usato/auto/dettaglio"

# ============================================================
# USATO → MARCHE (DELTA-ONLY)
# ============================================================

def sync_usato_marche():
    logger.info("[USATO][MARCHE] START")

    data = asyncio.run(motornet_get(USATO_MARCHE_URL))
    marche = data.get("marche", [])

    if not marche:
        logging.info("[USATO][MARCHE] NOTHING TO DO")
        return

    inserted = 0
    with DBSession() as db:
        for m in marche:
            res = db.execute(
                text("""
                    INSERT INTO mnet_marche_usato (acronimo, nome, logo)
                    SELECT
                        CAST(:acronimo AS varchar),
                        CAST(:nome AS varchar),
                        CAST(:logo AS varchar)
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM mnet_marche_usato
                        WHERE acronimo = CAST(:acronimo AS varchar)
                    )
                """),
                {
                    "acronimo": m.get("acronimo"),
                    "nome": m.get("nome"),
                    "logo": m.get("logo"),
                },
            )
            if res.rowcount == 1:
                inserted += 1

    logger.info("[USATO][MARCHE] DONE (new=%d, total_seen=%d)", inserted, len(marche))


# ============================================================
# USATO → ANNI (DELTA-ONLY, DRIVER TEMPORALE)
# ============================================================

def sync_usato_anni():
    """
    Inserisce SOLO nuove combinazioni (marca, anno, mese).
    Fonte di verità temporale per tutta la catena USATO.
    """
    logger.info("[USATO][ANNI] START")

    # strategia: mese corrente
    today = date.today()
    anno, mese = today.year, today.month

    inserted = 0
    with DBSession() as db:
        # marche già note (USATO)
        rows = db.execute(
            text("SELECT acronimo FROM mnet_marche_usato ORDER BY acronimo")
        ).fetchall()

        for (acronimo,) in rows:
            res = db.execute(
                text("""
                    INSERT INTO mnet_anni_usato (marca_acronimo, anno, mese)
                    SELECT
                        CAST(:marca AS varchar),
                        CAST(:anno AS int),
                        CAST(:mese AS int)
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM mnet_anni_usato
                        WHERE marca_acronimo = CAST(:marca AS varchar)
                          AND anno = CAST(:anno AS int)
                          AND mese = CAST(:mese AS int)
                    )
                """),
                {
                    "marca": acronimo,
                    "anno": anno,
                    "mese": mese,
                },
            )
            if res.rowcount == 1:
                inserted += 1

    logger.info("[USATO][ANNI] DONE (new=%d)", inserted)


# ============================================================
# USATO → MODELLI (DELTA-ONLY, PER ANNO)
# ============================================================

def sync_usato_modelli():
    logger.info("[USATO][MODELLI] START")

    with DBSession() as db:
        combos = db.execute(
            text("""
                SELECT marca_acronimo, anno
                FROM mnet_anni_usato
                WHERE anno >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
                ORDER BY marca_acronimo, anno

            """)
        ).fetchall()

    inserted = 0
    for marca, anno in combos:
        try:
            data = asyncio.run(
                motornet_get(
                    f"{USATO_MODELLI_URL}?codice_marca={marca}&anno={anno}&libro=false"
                )
            )
            modelli = data.get("modelli", [])
            if not modelli:
                continue

            with DBSession() as db:
                for m in modelli:
                    cod_desc = (m.get("codDescModello") or {}).get("codice")
                    gamma = (m.get("gammaModello") or {}).get("codice")
                    if not cod_desc or not gamma:
                        continue

                    res = db.execute(
                        text("""
                            INSERT INTO mnet_modelli_usato (
                                marca_acronimo, codice_desc_modello, codice_modello,
                                descrizione, descrizione_dettagliata,
                                gruppo_storico, inizio_produzione, fine_produzione,
                                inizio_commercializzazione, fine_commercializzazione,
                                segmento, tipo, serie_gamma, created_at
                            )
                            SELECT
                                CAST(:marca AS varchar),
                                CAST(:cod_desc AS varchar),
                                CAST(:gamma AS varchar),
                                CAST(:descr AS varchar),
                                CAST(:descr_det AS text),
                                CAST(:gruppo AS varchar),
                                CAST(:ip AS date),
                                CAST(:fp AS date),
                                CAST(:ic AS date),
                                CAST(:fc AS date),
                                CAST(:segmento AS varchar),
                                CAST(:tipo AS varchar),
                                CAST(:serie AS varchar),
                                CURRENT_DATE
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM mnet_modelli_usato
                                WHERE marca_acronimo = CAST(:marca AS varchar)
                                    AND codice_modello = CAST(:gamma AS varchar)
                            )

                        """),
                        {
                            "marca": marca,
                            "cod_desc": cod_desc,
                            "gamma": gamma,
                            "descr": (m.get("codDescModello") or {}).get("descrizione"),
                            "descr_det": (m.get("gammaModello") or {}).get("descrizione"),
                            "gruppo": (m.get("gruppoStorico") or {}).get("descrizione"),
                            "ip": m.get("inizioProduzione"),
                            "fp": m.get("fineProduzione"),
                            "ic": m.get("inizioCommercializzazione"),
                            "fc": m.get("fineCommercializzazione"),
                            "segmento": None,
                            "tipo": None,
                            "serie": (m.get("serieGamma") or {}).get("descrizione"),
                        },
                    )
                    if res.rowcount == 1:
                        inserted += 1
        except Exception:
            logging.exception("[USATO][MODELLI] FAILED %s-%s", marca, anno)
            continue

    logger.info("[USATO][MODELLI] DONE (new=%d)", inserted)


# ============================================================
# USATO → ALLESTIMENTI (DELTA-ONLY)
# ============================================================

def sync_usato_allestimenti():
    logger.info("[USATO][ALLESTIMENTI] START")

    with DBSession() as db:
        rows = db.execute(
            text("""
                SELECT DISTINCT m.marca_acronimo, a.anno, m.codice_modello
                FROM mnet_modelli_usato m
                JOIN mnet_anni_usato a ON a.marca_acronimo = m.marca_acronimo
                WHERE a.anno >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
                ORDER BY m.marca_acronimo, a.anno, m.codice_modello

            """)
        ).fetchall()

    inserted = 0
    for marca, anno, codice_modello in rows:
        try:
            data = asyncio.run(
                motornet_get(
                    f"{USATO_VERSIONI_URL}?codice_modello={codice_modello}&anno={anno}&libro=false"
                )
            )
            versioni = data.get("versioni", [])
            if not versioni:
                continue

            with DBSession() as db:
                for v in versioni:
                    codice_uni = v.get("codiceMotornet")
                    if not codice_uni:
                        continue

                    res = db.execute(
                        text("""
                            INSERT INTO mnet_allestimenti_usato (
                                codice_motornet_uni, acronimo_marca, codice_modello, versione,
                                inizio_produzione, fine_produzione,
                                inizio_commercializzazione, fine_commercializzazione,
                                codice_eurotax
                            )
                            SELECT
                                CAST(:codice AS varchar),
                                CAST(:marca AS varchar),
                                CAST(:modello AS varchar),
                                CAST(:versione AS varchar),
                                CAST(:ip AS date),
                                CAST(:fp AS date),
                                CAST(:ic AS date),
                                CAST(:fc AS date),
                                CAST(:eurotax AS varchar)
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM mnet_allestimenti_usato
                                WHERE codice_motornet_uni = CAST(:codice AS varchar)
                            )
                        """),
                        {
                            "codice": codice_uni,
                            "marca": marca,
                            "modello": codice_modello,
                            "versione": v.get("nome"),
                            "ip": v.get("inizioProduzione"),
                            "fp": v.get("fineProduzione"),
                            "ic": v.get("da"),
                            "fc": v.get("a"),
                            "eurotax": v.get("codiceEurotax"),
                        },
                    )
                    if res.rowcount == 1:
                        inserted += 1
        except Exception:
            logger.exception("[USATO][ALLESTIMENTI] FAILED %s-%s-%s", marca, anno, codice_modello)

            continue

    logger.info("[USATO][ALLESTIMENTI] DONE (new=%d)", inserted)


# ============================================================
# USATO → DETTAGLI (DELTA-ONLY)
# ============================================================
def to_float_or_none(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        v = v.strip()
        try:
            return float(v)
        except ValueError:
            return None
    return None


def to_bool(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v == 1
    if isinstance(v, str):
        v = v.strip().lower()
        if v in ("1", "true", "t", "s", "si", "yes"):
            return True
        if v in ("0", "false", "f", "n", "no", ""):
            return False
    return None

def build_params(modello: dict, codice: str) -> dict:
    return {
        "codice": codice,
        "modello": modello.get("modello"),
        "allestimento": modello.get("allestimento"),
        "immagine": modello.get("immagine"),
        "codice_costruttore": modello.get("codiceCostruttore"),
        "codice_motore": modello.get("codiceMotore"),
        "prezzo_listino": modello.get("prezzoListino"),
        "prezzo_accessori": modello.get("prezzoAccessori"),
        "data_listino": modello.get("dataListino"),
        "marca_nome": (modello.get("marca") or {}).get("nome"),
        "marca_acronimo": (modello.get("marca") or {}).get("acronimo"),
        "gamma_codice": (modello.get("gammaModello") or {}).get("codice"),
        "gamma_descrizione": (modello.get("gammaModello") or {}).get("descrizione"),
        "gruppo_storico": (modello.get("gruppoStorico") or {}).get("descrizione"),
        "serie_gamma": (modello.get("serieGamma") or {}).get("descrizione"),
        "categoria": (modello.get("categoria") or {}).get("descrizione"),
        "segmento": (modello.get("segmento") or {}).get("descrizione"),
        "tipo": (modello.get("tipo") or {}).get("descrizione"),
        "tipo_motore": modello.get("tipoMotore"),
        "descrizione_motore": modello.get("descrizioneMotore"),
        "euro": modello.get("euro"),
        "cilindrata": modello.get("cilindrata"),
        "cavalli_fiscali": modello.get("cavalliFiscali"),
        "hp": modello.get("hp"),
        "kw": modello.get("kw"),
        "emissioni_co2": to_float_or_none(modello.get("emissioniCo2")),
        "consumo_urbano": to_float_or_none(modello.get("consumoUrbano")),
        "consumo_extraurbano": to_float_or_none(modello.get("consumoExtraurbano")),
        "consumo_medio": to_float_or_none(modello.get("consumoMedio")),
        "accelerazione": to_float_or_none(modello.get("accelerazione")),
        "velocita": modello.get("velocita"),
        "descrizione_marce": modello.get("descrizioneMarce"),
        "cambio": (modello.get("cambio") or {}).get("descrizione"),
        "trazione": (modello.get("trazione") or {}).get("descrizione"),
        "passo": modello.get("passo"),
        "porte": modello.get("porte"),
        "posti": modello.get("posti"),
        "altezza": modello.get("altezza"),
        "larghezza": modello.get("larghezza"),
        "lunghezza": modello.get("lunghezza"),
        "bagagliaio": modello.get("bagagliaio"),
        "pneumatici_anteriori": modello.get("pneumaticiAnteriori"),
        "pneumatici_posteriori": modello.get("pneumaticiPosteriori"),
        "coppia": modello.get("coppia"),
        "numero_giri": modello.get("numeroGiri"),
        "cilindri": modello.get("cilindri"),
        "valvole": modello.get("valvole"),
        "peso": modello.get("peso"),
        "peso_vuoto": modello.get("pesoVuoto"),
        "massa_p_carico": modello.get("massaPCarico"),
        "portata": modello.get("portata"),
        "tipo_guida": modello.get("tipoGuida"),
        "neo_patentati": to_bool(modello.get("neoPatentati")),
        "alimentazione": (modello.get("alimentazione") or {}).get("descrizione"),
        "architettura": (modello.get("architettura") or {}).get("descrizione"),
        "ricarica_standard": to_bool(modello.get("ricaricaStandard")),
        "ricarica_veloce": to_bool(modello.get("ricaricaVeloce")),
        "sospensioni_pneumatiche": to_bool(modello.get("sospPneum")),
        "emissioni_urbe": to_float_or_none(modello.get("emissUrbe")),
        "emissioni_extraurb": to_float_or_none(modello.get("emissExtraurb")),
        "descrizione_breve": modello.get("descrizioneBreve"),
        "peso_potenza": modello.get("pesoPotenza"),
        "volumi": modello.get("volumi"),
        "ridotte": to_bool(modello.get("ridotte")),
        "paese_prod": modello.get("paeseProd"),
    }


async def _sync_usato_dettagli_async(db, codici):
    processed = 0
    inserted = 0
    seen = len(codici)

    for codice in codici:

        MAX_RETRY = 5
        retry = 0
        success = False

        while retry <= MAX_RETRY:
            try:
                data = await motornet_get(
                    f"{USATO_DETTAGLIO_URL}?codice_motornet={codice}"
                )

                modello = data.get("modello")
                if not modello:
                    break  # codice valido ma senza modello → vai avanti
                    
                params = build_params(modello, codice)

                res = db.execute(
                    text("""
                        INSERT INTO mnet_dettagli_usato (
                            codice_motornet_uni, modello, allestimento, immagine,
                            codice_costruttore, codice_motore,
                            prezzo_listino, prezzo_accessori, data_listino,
                            marca_nome, marca_acronimo,
                            gamma_codice, gamma_descrizione, gruppo_storico, serie_gamma,
                            categoria, segmento, tipo,
                            tipo_motore, descrizione_motore, euro, cilindrata, cavalli_fiscali, hp, kw,
                            emissioni_co2, consumo_urbano, consumo_extraurbano, consumo_medio,
                            accelerazione, velocita,
                            descrizione_marce, cambio, trazione, passo,
                            porte, posti, altezza, larghezza, lunghezza,
                            bagagliaio, pneumatici_anteriori, pneumatici_posteriori,
                            coppia, numero_giri, cilindri, valvole, peso, peso_vuoto,
                            massa_p_carico, portata, tipo_guida, neo_patentati,
                            alimentazione, architettura, ricarica_standard, ricarica_veloce,
                            sospensioni_pneumatiche, emissioni_urbe, emissioni_extraurb, descrizione_breve,
                            peso_potenza, volumi, ridotte, paese_prod
                        )
                        SELECT
                            :codice, :modello, :allestimento, :immagine,
                            :codice_costruttore, :codice_motore,
                            :prezzo_listino, :prezzo_accessori, :data_listino,
                            :marca_nome, :marca_acronimo,
                            :gamma_codice, :gamma_descrizione, :gruppo_storico, :serie_gamma,
                            :categoria, :segmento, :tipo,
                            :tipo_motore, :descrizione_motore, :euro, :cilindrata, :cavalli_fiscali, :hp, :kw,
                            :emissioni_co2, :consumo_urbano, :consumo_extraurbano, :consumo_medio,
                            :accelerazione, :velocita,
                            :descrizione_marce, :cambio, :trazione, :passo,
                            :porte, :posti, :altezza, :larghezza, :lunghezza,
                            :bagagliaio, :pneumatici_anteriori, :pneumatici_posteriori,
                            :coppia, :numero_giri, :cilindri, :valvole, :peso, :peso_vuoto,
                            :massa_p_carico, :portata, :tipo_guida, :neo_patentati,
                            :alimentazione, :architettura, :ricarica_standard, :ricarica_veloce,
                            :sospensioni_pneumatiche, :emissioni_urbe, :emissioni_extraurb, :descrizione_breve,
                            :peso_potenza, :volumi, :ridotte, :paese_prod
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM mnet_dettagli_usato

                            WHERE codice_motornet_uni = :codice
                        )
                    """),
                    params,

                )

                if res.rowcount == 1:
                    inserted += 1
                    db.commit()
                    success = True
                
                    await asyncio.sleep(random.uniform(0.7, 0.9))




                break  # SUCCESSO → esci dal while


            except RuntimeError as e:
                if "429" in str(e):
                    retry += 1
                    wait = min(30 * retry, 300)
                    logger.warning(
                        "[USATO][DETTAGLI] 429 su %s → retry %d (sleep %ds)",
                        codice, retry, wait
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(
                        "[USATO][DETTAGLI] HARD FAIL %s → skipped (%s)",
                        codice,
                        str(e),
                    )
                    break


        processed += 1
        if not success and retry >= MAX_RETRY:
            logger.error(
                "[USATO][DETTAGLI] SKIPPED %s after retries",
                codice,
            )
  


        if processed % 100 == 0:
            logger.info(
                "[USATO][DETTAGLI] progress %d / %d (%.1f%%)",
                processed, seen, processed * 100 / seen
            )
            await asyncio.sleep(3)



    return processed, inserted, 0


def sync_usato_dettagli():
    logger.info("[USATO][DETTAGLI] START")

    with DBSession() as db:
        rows = db.execute(
            text("""
                SELECT DISTINCT a.codice_motornet_uni
                FROM mnet_allestimenti_usato a
                LEFT JOIN mnet_dettagli_usato d
                  ON d.codice_motornet_uni = a.codice_motornet_uni
                JOIN mnet_anni_usato y
                  ON y.marca_acronimo = a.acronimo_marca
                WHERE y.anno >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
                  AND d.codice_motornet_uni IS NULL
                ORDER BY a.codice_motornet_uni;

            """)
        ).fetchall()

        codici = [r[0] for r in rows]
        if not codici:
            logger.info("[USATO][DETTAGLI] NOTHING TO DO")
            return

        processed, inserted, updated = asyncio.run(
            _sync_usato_dettagli_async(db, codici)
        )

        db.commit()

        logger.info(
            "[USATO][DETTAGLI] DONE processed=%d new=%d updated=%d",
            processed,
            inserted,
            updated,
        )

# ============================================================
# STOCK → VEHICLE_VERSIONS_CM (cod_versione_cm → Motornet mapping)
# Robust production worker (delta-only + safe upsert)
# ============================================================

import asyncio
import random
import json

import logging
from typing import Any, Dict, Optional, Tuple, List

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

# NOTE: usa i tuoi import reali (come negli altri job)
# from app.database import DBSession
# from app.external.motornet import motornet_get
# from app.config import USATO_COSTRUTTORE_URL  # oppure costante locale

logger = logging.getLogger(__name__)

# endpoint usato: /usato/auto/costruttore?codice_costruttore=...
USATO_COSTRUTTORE_URL = (
    "https://webservice.motornet.it/api/v2_0/rest/public/usato/auto/costruttore"
)

# pacing: ~0.7–0.9s per richiesta (come richiesto)
PACE_MIN = 0.70
PACE_MAX = 0.90

import re

def normalize_cod_versione_cm(raw: str | None) -> str | None:
    """
    Normalizza il codice versione:
    - None → None
    - strip
    - uppercase
    - mantiene SOLO A–Z0–9
    - stringhe vuote → None
    """
    if not raw:
        return None

    norm = re.sub(r'[^A-Z0-9]', '', raw.upper())
    return norm or None


def _pick_best_version(resp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Sceglie la 'versione' migliore dal payload /auto/costruttore.
    Strategia:
    - se c'è 'versioni' e non è vuoto: usa la prima (per quel codice_costruttore è tipicamente 1)
    - altrimenti None
    """
    versioni = resp.get("versioni") or []
    if not versioni:
        return None
    return versioni[0]


def _extract_brand(resp: Dict[str, Any], v: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    marca = (v.get("marca") or {}) or {}
    brand_code = marca.get("acronimo")
    brand_name = marca.get("nome")
    if brand_code and brand_name:
        return brand_code, brand_name

    # fallback: resp["marche"][0]
    marche = resp.get("marche") or []
    if marche:
        brand_code = (marche[0] or {}).get("acronimo")
        brand_name = (marche[0] or {}).get("nome")
    return brand_code, brand_name


def _extract_model(resp: Dict[str, Any], v: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    vehicle_versions_cm richiede model_name NOT NULL.
    Usiamo:
    - model_code: v["codiceModello"] (spesso presente)
    - model_name: resp["versioniBase"][0]["gammaModello"]["descrizione"] se presente
      fallback: v["descrizioneModelloBreveCarrozzeria"] o "modelli"[0]["gammaModello"]["descrizione"]
    """
    model_code = v.get("codiceModello")

    model_name = None
    versioni_base = resp.get("versioniBase") or []
    if versioni_base:
        vb0 = versioni_base[0] or {}
        gamma = (vb0.get("gammaModello") or {}) or {}
        model_name = gamma.get("descrizione")

    if not model_name:
        model_name = v.get("descrizioneModelloBreveCarrozzeria")

    if not model_name:
        modelli = resp.get("modelli") or []
        if modelli:
            m0 = modelli[0] or {}
            gamma = (m0.get("gammaModello") or {}) or {}
            model_name = gamma.get("descrizione") or (m0.get("gruppoStorico") or {}).get("descrizione")

    return model_code, model_name


def _build_vehicle_versions_cm_row(
    cod_versione_cm: str,
    resp: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    v = _pick_best_version(resp)
    if not v:
        return None

    brand_code, brand_name = _extract_brand(resp, v)
    model_code, model_name = _extract_model(resp, v)

    # campi version
    vb = (v.get("versioneBase") or {}) or {}
    version_base_id = vb.get("id")
    version_base_name = vb.get("descrizione")

    version_name = v.get("versione") or v.get("versioneBase", {}).get("descrizione")

    codice_motornet = v.get("codiceMotornet")
    codice_eurotax = v.get("codiceEurotax")
    codice_costruttore = v.get("codiceCostruttore")

    # date
    production_start = v.get("inizioProduzione")
    production_end = v.get("fineProduzione")
    commercial_start = v.get("da")
    commercial_end = v.get("a")

    doors = v.get("porte")
    wheelbase_cm = v.get("passo")  # in Motornet è già "passo" (cm)
    list_price = v.get("prezzoVendita")

    # hard requirements: vehicle_versions_cm.brand_code, brand_name, model_name, version_name, raw_payload NOT NULL
    if not brand_code or not brand_name or not model_name or not version_name:
        logger.warning(
            "[VEHICLE_VERSIONS_CM] incomplete mandatory fields for %s -> brand(%s/%s) model_name(%s) version_name(%s)",
            cod_versione_cm,
            brand_code,
            brand_name,
            model_name,
            version_name,
        )
        # se mancano NOT NULL non inseriamo (evita crash)
        return None

    return {
        "cod_versione_cm": cod_versione_cm,
        "brand_code": brand_code,
        "brand_name": brand_name,
        "model_code": model_code,
        "model_name": model_name,
        "version_base_id": version_base_id,
        "version_base_name": version_base_name,
        "version_name": version_name,
        "codice_motornet": codice_motornet,
        "codice_eurotax": codice_eurotax,
        "codice_costruttore": codice_costruttore,
        "doors": doors,
        "wheelbase_cm": wheelbase_cm,
        "production_start": production_start,
        "production_end": production_end,
        "commercial_start": commercial_start,
        "commercial_end": commercial_end,
        "list_price": list_price,
        "raw_payload": resp,  # jsonb
    }


UPSERT_SQL = text(
    """
    INSERT INTO public.vehicle_versions_cm (
        cod_versione_cm,
        brand_code,
        brand_name,
        model_code,
        model_name,
        version_base_id,
        version_base_name,
        version_name,
        codice_motornet,
        codice_eurotax,
        codice_costruttore,
        doors,
        wheelbase_cm,
        production_start,
        production_end,
        commercial_start,
        commercial_end,
        list_price,
        raw_payload
    )
    VALUES (
        :cod_versione_cm,
        :brand_code,
        :brand_name,
        :model_code,
        :model_name,
        :version_base_id,
        :version_base_name,
        :version_name,
        :codice_motornet,
        :codice_eurotax,
        :codice_costruttore,
        :doors,
        :wheelbase_cm,
        CAST(:production_start AS date),
        CAST(:production_end AS date),
        CAST(:commercial_start AS date),
        CAST(:commercial_end AS date),
        :list_price,
        CAST(:raw_payload AS jsonb)
    )
    ON CONFLICT (cod_versione_cm) DO UPDATE SET
        -- aggiorna solo i buchi (non sovrascrive valori già presenti)
        brand_code          = COALESCE(vehicle_versions_cm.brand_code, EXCLUDED.brand_code),
        brand_name          = COALESCE(vehicle_versions_cm.brand_name, EXCLUDED.brand_name),
        model_code          = COALESCE(vehicle_versions_cm.model_code, EXCLUDED.model_code),
        model_name          = COALESCE(vehicle_versions_cm.model_name, EXCLUDED.model_name),
        version_base_id     = COALESCE(vehicle_versions_cm.version_base_id, EXCLUDED.version_base_id),
        version_base_name   = COALESCE(vehicle_versions_cm.version_base_name, EXCLUDED.version_base_name),
        version_name        = COALESCE(vehicle_versions_cm.version_name, EXCLUDED.version_name),
        codice_motornet      = COALESCE(vehicle_versions_cm.codice_motornet, EXCLUDED.codice_motornet),
        codice_eurotax       = COALESCE(vehicle_versions_cm.codice_eurotax, EXCLUDED.codice_eurotax),
        codice_costruttore   = COALESCE(vehicle_versions_cm.codice_costruttore, EXCLUDED.codice_costruttore),
        doors               = COALESCE(vehicle_versions_cm.doors, EXCLUDED.doors),
        wheelbase_cm        = COALESCE(vehicle_versions_cm.wheelbase_cm, EXCLUDED.wheelbase_cm),
        production_start    = COALESCE(vehicle_versions_cm.production_start, EXCLUDED.production_start),
        production_end      = COALESCE(vehicle_versions_cm.production_end, EXCLUDED.production_end),
        commercial_start    = COALESCE(vehicle_versions_cm.commercial_start, EXCLUDED.commercial_start),
        commercial_end      = COALESCE(vehicle_versions_cm.commercial_end, EXCLUDED.commercial_end),
        list_price          = COALESCE(vehicle_versions_cm.list_price, EXCLUDED.list_price),
        raw_payload         = COALESCE(vehicle_versions_cm.raw_payload, EXCLUDED.raw_payload),
        updated_at          = now()
    """
)
LINK_STOCK_SQL = text("""
    UPDATE public.vehicles_stock_sale s
    SET vehicle_version_cm_id = v.id
    FROM public.vehicle_versions_cm v
    WHERE s.vehicle_version_cm_id IS NULL
      AND s.cod_versione_cm = v.cod_versione_cm
""")



def _fetch_codici_da_stock(db) -> List[str]:
    """
    Delta-only:
    - prendi i DISTINCT cod_versione_cm dallo stock
    - escludi vuoti
    - includi solo quelli non presenti in vehicle_versions_cm
      oppure presenti ma con codice_motornet/codice_costruttore NULL (buchi)
    """
    rows = db.execute(
        text(
            """
            SELECT DISTINCT
                s.cod_versione_cm
            FROM public.vehicles_stock_sale s
            LEFT JOIN public.vehicle_versions_cm v
                   ON v.cod_versione_cm = s.cod_versione_cm
            WHERE s.is_active = true
              AND s.cod_versione_cm IS NOT NULL
              AND btrim(s.cod_versione_cm) <> ''
              AND (
                  v.id IS NULL
                  OR v.codice_motornet IS NULL
                  OR v.codice_costruttore IS NULL
              )
            ORDER BY s.cod_versione_cm
            """
        )
    ).fetchall()

    return list({
        normalize_cod_versione_cm(r[0])
        for r in rows
        if normalize_cod_versione_cm(r[0]) is not None
    })



async def _sync_vehicle_versions_cm_async(db, codici: List[str]) -> Tuple[int, int, int, int]:
    processed = 0
    upserted = 0
    skipped = 0
    failed = 0

    total = len(codici)

    for cod in codici:
        processed += 1

        # audit: il codice A QUESTO PUNTO è già normalizzato,
        # ma logghiamo se per qualche motivo non matcha il pattern
        if not re.fullmatch(r'[A-Z0-9]+', cod):
            logger.info(
                "[VEHICLE_VERSIONS_CM] normalized/filtered cod_versione_cm=%s",
                cod,
            )

        # chiamata Motornet
        url = f"{USATO_COSTRUTTORE_URL}?codice_costruttore={cod}"

        try:
            resp = await motornet_get(url)

            row = _build_vehicle_versions_cm_row(cod, resp)
            if not row:
                skipped += 1
            else:
                db.execute(
                    UPSERT_SQL,
                    {
                        **row,
                        "raw_payload": json.dumps(row["raw_payload"]),
                    },
                )
                db.commit()
                upserted += 1

        except Exception as e:
            failed += 1
            logger.exception(
                "[VEHICLE_VERSIONS_CM] FAILED cod_versione_cm=%s err=%s",
                cod,
                str(e),
            )

        # pacing
        await asyncio.sleep(random.uniform(PACE_MIN, PACE_MAX))

        if processed % 50 == 0:
            logger.info(
                "[VEHICLE_VERSIONS_CM] progress %d/%d upserted=%d skipped=%d failed=%d",
                processed,
                total,
                upserted,
                skipped,
                failed,
            )

    return processed, upserted, skipped, failed



def sync_vehicle_versions_cm_from_stock() -> None:
    """
    Entry-point sync per APScheduler.
    """
    logger.info("[VEHICLE_VERSIONS_CM] START")

    with DBSession() as db:
        codici = _fetch_codici_da_stock(db)

    if not codici:
        logger.info("[VEHICLE_VERSIONS_CM] NOTHING TO DO")
        return

    # eseguo in una sessione dedicata per le write
    with DBSession() as db:
        processed, upserted, skipped, failed = asyncio.run(_sync_vehicle_versions_cm_async(db, codici))
        
        # --------------------------------------------------
        # LINK STOCK → VEHICLE_VERSIONS_CM (idempotente)
        # --------------------------------------------------
        res = db.execute(LINK_STOCK_SQL)
        db.commit()

        linked = res.rowcount
        logger.info(
            "[VEHICLE_VERSIONS_CM] linked stock rows=%d",
            linked,
        )


    logger.info(
        "[VEHICLE_VERSIONS_CM] DONE processed=%d upserted=%d skipped=%d failed=%d",
        processed,
        upserted,
        skipped,
        failed,
    )

# ============================================================
# Local execution (manual run)
# ============================================================
if __name__ == "__main__":
    import logging
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    logger = logging.getLogger(__name__)

    logger.info("[LOCAL] Running sync_vehicle_versions_cm_from_stock")

    try:
        sync_vehicle_versions_cm_from_stock()
        logger.info("[LOCAL] Completed successfully")

    except KeyboardInterrupt:
        logger.warning("[LOCAL] Interrupted by user")

    except Exception:
        logger.exception("[LOCAL] Failed")
        sys.exit(1)
