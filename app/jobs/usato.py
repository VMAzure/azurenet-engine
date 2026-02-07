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
                        INSERT INTO mnet_dettagli_usato_shadow (
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
                            FROM mnet_dettagli_usato_shadow

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
                JOIN mnet_anni_usato y
                  ON y.marca_acronimo = a.acronimo_marca
                WHERE y.anno >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
                ORDER BY a.codice_motornet_uni
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

