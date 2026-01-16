import re
from typing import Optional, Tuple, Dict
from sqlalchemy import text
from app.database import SessionLocal


# ============================================================
# CONFIGURAZIONE CENTRALE (CONGELATA)
# ============================================================

# Marche che NON devono mai passare dal matcher generico
# perché hanno (o avranno) un resolver dedicato
DEDICATED_RESOLVERS = {
    "mercedes": "mercedes",
    "mercedes-benz": "mercedes",
    "audi": "audi",
    "bmw": "bmw",
    "volkswagen": "vw",
    "renault": "renault",
}


# Confidence base per matcher generico
GENERIC_CONFIDENCE = 0.92


# ============================================================
# MATCHER GENERICO (TESTUALE, CONSERVATIVO)
# ============================================================

BLACKLIST = {
    "quattro",
    "allroad",
    "sportback",
    "variant",
    "sw",
    "break",
    "touring",
    "cabrio",
    "coupe",
    "cross",
}

ENGINE_SUFFIX_RE = r"(d|i|tdi|cdti|hdi|mhev|hybrid|phev|ev|e)$"


def normalize(text: str) -> str:
    return text.lower().strip()


def strip_blacklist(text: str) -> str:
    for b in BLACKLIST:
        text = re.sub(rf"\b{b}\b", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def model_tokens(text: str):
    return re.findall(r"[a-z]+", text)


def normalize_model_token(model_name: str) -> str:
    """
    Estrae SOLO token alfabetici dal nome modello AS24.
    Il matcher generico NON lavora su numeri.
    """
    tokens = model_tokens(normalize(model_name))
    return tokens[-1] if tokens else ""


def match_generic_model(
    allestimento: str,
    as24_model_names: list[str]
) -> Tuple[Optional[str], str, float]:

    if not allestimento:
        return None, "NONE", 0.0

    text = strip_blacklist(normalize(allestimento))
    matches = []

    for model in as24_model_names:
        token = normalize_model_token(model)
        if not token:
            continue

        pattern = rf"(^|[^a-z]){re.escape(token)}([^a-z]|$)"
        if re.search(pattern, text):
            matches.append(model)

    if not matches:
        return None, "NONE", 0.0

    matches = list(set(matches))

    if len(matches) == 1:
        return matches[0], "GENERIC_OK", GENERIC_CONFIDENCE

    return None, "AMBIGUOUS", 0.0


# ============================================================
# DISPATCHER RESOLVER (ESTENDIBILE)
# ============================================================

def resolve_model(
    marca: str,
    allestimento: str,
    as24_model_names: list[str]
):
    marca = marca.lower()

    if marca in DEDICATED_RESOLVERS:
        resolver_name = DEDICATED_RESOLVERS[marca]

        # Resolver dedicati (quando esistono)
        if marca == "mercedes":
            from app.jobs.autoscout_model_alignment_job.mercedes_resolver import match_mercedes_model
            return match_mercedes_model(allestimento, as24_model_names)

        if marca == "audi":
            from app.jobs.autoscout_model_alignment_job.audi_resolver import match_audi_model
            return match_audi_model(allestimento, as24_model_names)

        if resolver_name == "bmw":
            from app.jobs.autoscout_model_alignment_job.bmw_resolver import match_bmw_model
            return match_bmw_model(allestimento, as24_model_names)

        if resolver_name == "vw":
            from app.jobs.autoscout_model_alignment_job.vw_resolver import match_vw_model
            return match_vw_model(allestimento, as24_model_names)

        if resolver_name == "renault":
            from app.jobs.autoscout_model_alignment_job.renault_resolver import match_renault_model
            return match_renault_model(allestimento, as24_model_names)




    return match_generic_model(allestimento, as24_model_names)


# ============================================================
# JOB PRINCIPALE (DB → DB, IDOTEMPENTE)
# ============================================================

def populate_autoscout_model_map_v2():
    db = SessionLocal()

    # --------------------------------------------------------
    # 1) CARICA MODELLI AS24 (UNA SOLA QUERY)
    # --------------------------------------------------------
    as24_rows = db.execute(text("""
        SELECT
            autoscout_make_id,
            lower(autoscout_make_name) AS make_name,
            autoscout_model_id,
            autoscout_model_name
        FROM autoscout_reference_models
        WHERE is_active = true
    """)).mappings().all()

    as24_by_make: Dict[str, list] = {}
    for r in as24_rows:
        as24_by_make.setdefault(r["make_name"], []).append(r)

    print(f"[INIT] AS24 makes loaded: {len(as24_by_make)}")

    # --------------------------------------------------------
    # 2) CARICA AUTO MNET NON ANCORA MAPPATE
    # --------------------------------------------------------
    rows = db.execute(text("""
        SELECT
            du.codice_motornet_uni,
            mu.nome AS marca_mnet,
            du.allestimento
        FROM mnet_dettagli_usato du
        JOIN mnet_marche_usato mu
          ON mu.acronimo = LEFT(du.codice_motornet_uni, 3)
        LEFT JOIN autoscout_model_map_v2 m
          ON m.codice_motornet_uni = du.codice_motornet_uni
        WHERE du.allestimento IS NOT NULL
          AND trim(du.allestimento) <> ''
          AND m.codice_motornet_uni IS NULL
    """)).mappings().all()

    print(f"[INIT] MNET rows to process: {len(rows)}")

    # --------------------------------------------------------
    # 3) LOOP + INSERT
    # --------------------------------------------------------
    inserted = 0
    skipped = 0

    for i, row in enumerate(rows, start=1):
        codice_uni = row["codice_motornet_uni"]
        marca = row["marca_mnet"].lower()
        allestimento = row["allestimento"]

        as24_candidates = as24_by_make.get(marca)
        if not as24_candidates:
            skipped += 1
            continue

        model_names = [r["autoscout_model_name"] for r in as24_candidates]

        model_name, reason, confidence = resolve_model(
            marca,
            allestimento,
            model_names
        )

        if not model_name:
            skipped += 1
            continue

        as24_model = next(
            r for r in as24_candidates
            if r["autoscout_model_name"] == model_name
        )

        db.execute(text("""
            INSERT INTO autoscout_model_map_v2 (
                codice_motornet_uni,
                mnet_marca,
                mnet_allestimento,
                as24_make_id,
                as24_model_id,
                source,
                confidence
            )
            VALUES (
                :codice_uni,
                :marca,
                :allestimento,
                :make_id,
                :model_id,
                :source,
                :confidence
            )
            ON CONFLICT (codice_motornet_uni)
            DO NOTHING
        """), {
            "codice_uni": codice_uni,
            "marca": marca,
            "allestimento": allestimento,
            "make_id": as24_model["autoscout_make_id"],
            "model_id": as24_model["autoscout_model_id"],
            "source": f"{marca}_resolver" if marca in DEDICATED_RESOLVERS else "generic_resolver_v1",
            "confidence": confidence
        })

        inserted += 1

        if i % 500 == 0:
            db.commit()
            print(f"[PROGRESS] processed={i} inserted={inserted} skipped={skipped}")

    db.commit()
    db.close()

    print(f"[DONE] processed={len(rows)} inserted={inserted} skipped={skipped}")


if __name__ == "__main__":
    populate_autoscout_model_map_v2()
