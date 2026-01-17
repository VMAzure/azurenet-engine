from datetime import date

def build_minimal_payload(
    auto: dict,
    usatoin: dict,
    as24_make_id: int,
    as24_model_id: int,
    as24_bodytype_id: int,
    as24_primary_fuel_type: int,
    as24_fuel_category: str,
    as24_transmission: str,
    as24_power: int | None,
    as24_cylinder_capacity: int | None,
    as24_cylinder_count: int | None,
    as24_empty_weight: int | None,
    as24_seat_count: int | None,
    as24_door_count: int | None,
    as24_last_service_date: str | None,
    as24_description: str | None,
    as24_equipment_ids: list[int] | None,
    as24_has_full_service_history: bool | None,
    as24_model_version: str | None = None,

) -> dict:

    """
    Payload CREATE V1 AutoScout24 (professionale, produzione-safe).
    - availabilityType=1 (Immediata) hardcoded
    - bodyType risolto a monte (DB-driven, production-safe)
    - bodyColorName sempre passato dal DB
    - IVA gestita correttamente via PublicPrice
    - fuel (primaryFuelType + fuelCategory) risolti a monte
    - transmission risolto a monte (AS24 enum)

    """

    # -----------------------------
    # First registration YYYY-MM
    # -----------------------------
    year = auto.get("anno_immatricolazione")
    month = auto.get("mese_immatricolazione") or 1
    if not year:
        raise ValueError("anno_immatricolazione mancante")

    first_reg = f"{year}-{month:02d}"

    # -----------------------------
    # Mileage
    # -----------------------------
    mileage = int(auto.get("km_certificati") or 0)
    if mileage < 0:
        raise ValueError("km_certificati non validi")

    # -----------------------------
    # Price + VAT (PublicPrice)
    # -----------------------------
    prezzo_lordo = int(usatoin.get("prezzo_vendita") or 0)
    if prezzo_lordo <= 0:
        raise ValueError("prezzo_vendita non valido per AutoScout24")

    iva_esposta = bool(usatoin.get("iva_esposta"))

    public_price = {
        "price": prezzo_lordo,
        "currency": "EUR",
        "isNegotiable": False,
        "isTaxDeductible": iva_esposta,
    }

    # Se IVA esposta, passiamo netPrice e vatRate
    if iva_esposta:
        # NOTA: aliquota IVA va presa da config/DB se disponibile.
        # Qui assumiamo che sia già definita altrove (es. 22).
        vat_rate = int(usatoin.get("vat_rate") or 22)

        net_price = int(round(prezzo_lordo / (1 + vat_rate / 100)))
        public_price.update({
            "netPrice": net_price,
            "vatRate": vat_rate,
        })

    # -----------------------------
    # Payload finale
    # -----------------------------
    payload = {
        "vehicleType": "C",
        "offerType": "U",

        # ID AS24
        "make": as24_make_id,
        "model": as24_model_id,

        "firstRegistrationDate": first_reg,
        "mileage": mileage,

        # Fuel (risolto a monte, DB-driven)
        "fuel": {
            "primaryFuelType": as24_primary_fuel_type,
            "fuelCategory": as24_fuel_category,
        },

        # Transmission (AS24 enum, risolto a monte)
        "transmission": as24_transmission,

        # Availability (congelata)
        "availability": {
            "availabilityType": 1
        },

        # BodyType (risolto a monte)
        "bodyType": as24_bodytype_id,

        # Colore esterno
        "bodyColorName": auto.get("colore") or "",

        # Prezzi
        "prices": {
            "public": public_price
        },

        # Publication
        "publication": {
            "status": "Active",
            "channels": [{"id": "AS24"}],
        },
    }
    if as24_model_version:
        payload["modelVersion"] = as24_model_version

    # -----------------------------
    # Dati tecnici veicolo
    # -----------------------------

    if as24_power is not None:
        payload["power"] = as24_power
        payload["powerUnit"] = "kW"

    if as24_cylinder_capacity is not None:
        payload["cylinderCapacity"] = as24_cylinder_capacity
        payload["cylinderCapacityUnit"] = "m3"

    if as24_cylinder_count is not None:
        payload["cylinderCount"] = as24_cylinder_count

    if as24_empty_weight is not None:
        payload["emptyWeight"] = as24_empty_weight
        payload["emptyWeightUnit"] = "kg"

    if as24_seat_count is not None:
        payload["seatCount"] = as24_seat_count

    if as24_door_count is not None:
        payload["doorCount"] = as24_door_count

    if as24_last_service_date:
        payload["lastTechnicalServiceDate"] = as24_last_service_date

    if as24_description:
        payload["description"] = as24_description

    if as24_equipment_ids:
        payload["equipment"] = as24_equipment_ids

    if as24_has_full_service_history is not None:
        payload["hasFullServiceHistory"] = as24_has_full_service_history



    payload["condition"] = {
        "hadAccident": False
    }

    return payload
