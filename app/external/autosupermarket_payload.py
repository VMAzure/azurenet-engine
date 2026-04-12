"""AutoSuperMarket (ASM) — payload builder per annunci usato."""

import logging

logger = logging.getLogger(__name__)

# Mappa alimentazione Motornet → ASM fuel type
FUEL_MAP = {
    "Benzina": "gasoline",
    "Diesel": "diesel",
    "GPL": "lpg",
    "Metano": "cng",
    "Elettrica": "electric",
    "Ibrida": "hybrid",
    "Ibrida Benzina": "hybrid",
    "Ibrida Diesel": "hybrid",
    "Plug-in Hybrid": "plug-in-hybrid",
    "Idrogeno": "hydrogen",
}

# Mappa cambio Motornet → ASM gearbox
GEARBOX_MAP = {
    "Manuale": "manual",
    "Automatico": "automatic",
    "Automatica": "automatic",
    "Sequenziale": "automatic",
    "Robotizzato": "automatic",
}

# Mappa carrozzeria Motornet → ASM body type
BODY_MAP = {
    "Berlina": "sedan",
    "Station Wagon": "station-wagon",
    "Monovolume": "van",
    "SUV": "suv",
    "Coupé": "coupe",
    "Coupe": "coupe",
    "Cabriolet": "convertible",
    "Cabrio": "convertible",
    "Pick-Up": "pickup",
    "Furgone": "van",
    "Citycar": "city-car",
    "Crossover": "suv",
}

# Mappa normativa euro Motornet → ASM
EURO_MAP = {
    "EURO_1": "1",
    "EURO_2": "2",
    "EURO_3": "3",
    "EURO_4": "4",
    "EURO_5": "5",
    "EURO_6": "6",
    "EURO_6B": "6",
    "EURO_6C": "6C",
    "EURO_6D": "6D",
    "EURO_6D_TEMP": "6D-TEMP",
    "EURO_6E": "6E",
}


def _to_int(val) -> int | None:
    try:
        if val is None:
            return None
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


def build_asm_payload(
    auto: dict,
    usatoin: dict,
    det_base: dict,
    det_auto: dict | None,
    dealer_asm_id: str,
    images: list[str] | None = None,
) -> dict:
    """
    Costruisce il payload JSON per POST/PATCH su AutoSuperMarket.

    auto: row da azlease_usatoauto
    usatoin: row da azlease_usatoin
    det_base: row da v_mnet_dettagli_unificati
    det_auto: row da mnet_dettagli_usato (solo per AUTO, None per VIC)
    dealer_asm_id: ID dealer su ASM
    images: lista URL immagini pubbliche
    """

    marca = det_base.get("marca") or ""
    modello = det_base.get("modello") or ""
    allestimento = det_base.get("allestimento") or usatoin.get("alias_allestimento") or ""

    # Titolo annuncio
    title = f"{marca} {modello}".strip()
    if allestimento:
        title = f"{title} {allestimento}"

    # Alimentazione
    alimentazione_raw = ""
    if det_auto:
        alimentazione_raw = det_auto.get("alimentazione") or ""
    fuel = FUEL_MAP.get(alimentazione_raw, "gasoline")

    # Cambio
    cambio_raw = ""
    if det_auto:
        cambio_raw = det_auto.get("cambio") or ""
    gearbox = GEARBOX_MAP.get(cambio_raw, "manual")

    # Potenza
    power = _to_int(auto.get("kw_override")) or _to_int(det_auto.get("kw") if det_auto else None)

    # Cilindrata
    capacity = _to_int(det_auto.get("cilindrata") if det_auto else None)

    # Carrozzeria
    tipo_raw = det_auto.get("tipo", "") if det_auto else ""
    body = BODY_MAP.get(tipo_raw, "sedan")

    # Posti e porte
    seats = _to_int(det_auto.get("posti") if det_auto else None) or 5
    doors = _to_int(det_auto.get("porte") if det_auto else None) or 5

    # Colore
    color = auto.get("colore") or "gray"

    # Normativa euro
    euro_raw = auto.get("eu_emission_directive") or ""
    emission_class = EURO_MAP.get(euro_raw, "6")

    # Km
    km = _to_int(auto.get("km_certificati")) or 0

    # Data immatricolazione
    anno = auto.get("anno_immatricolazione")
    mese = auto.get("mese_immatricolazione") or 1
    reg_date = None
    if anno:
        reg_date = f"{anno}-{int(mese):02d}-01"

    # Prezzo
    prezzo = _to_int(usatoin.get("prezzo_vendita")) or 0

    # Descrizione
    descrizione = usatoin.get("descrizione") or ""

    payload = {
        "brand": marca,
        "model": modello,
        "vehicle": {
            "detail": {
                "body": body,
                "seats": seats,
                "doors": doors,
                "externalColor": color.lower() if color else "gray",
            },
            "engine": {
                "gearbox": gearbox,
                "power": power or 0,
            },
            "environment": {
                "fuel": fuel,
                "emissionClass": emission_class,
            },
            "status": {
                "category": "used",
                "km": km,
            },
        },
        "price": prezzo,
        "dealer": f"/dealers/{dealer_asm_id}",
        "status": "published",
        "title": title,
        "description": descrizione,
    }

    if capacity:
        payload["vehicle"]["engine"]["capacity"] = capacity

    if reg_date:
        payload["vehicle"]["status"]["registrationDate"] = reg_date

    if images:
        payload["images"] = images

    return payload
