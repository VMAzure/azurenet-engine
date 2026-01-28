from datetime import date, datetime
import re


AS24_EURO_MAP = {
    "EURO_1": "1",
    "EURO_2": "2",
    "EURO_3": "3",
    "EURO_4": "4",
    "EURO_5": "5",
    "EURO_6": "6",
    "EURO_6B": "11",
    "EURO_6C": "7",
    "EURO_6D": "8",
    "EURO_6D_TEMP": "9",
    "EURO_6E": "10",
}

def normalize_year_month(value) -> str | None:
    if value is None:
        return None

    if isinstance(value, (date, datetime)):
        return f"{value.year}-{value.month:02d}"

    if isinstance(value, str):
        value = value.strip()
        if re.match(r"^\d{4}-\d{2}$", value):
            return value
        raise ValueError(f"lastTechnicalServiceDate non valido: {value}")

    raise ValueError(f"Tipo lastTechnicalServiceDate non supportato: {type(value)}")


def build_minimal_payload(
    vehicle_type: str,  # "C" | "X"
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
    alloy_wheel_size: int | None = None,   
    as24_drivetrain: str | None = None,
    as24_warranty_months: int | None = None,
    as24_previous_owner_count: int | None = None,
    autoscout_attrs: dict | None = None,
    as24_co2: float | None = None,
    as24_consumo_urbano: float | None = None,
    as24_consumo_extraurbano: float | None = None,
    as24_consumo_medio: float | None = None,
    gear_count: int | None = None,


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
    if vehicle_type == "C":
        if not as24_make_id or not as24_model_id:
            raise ValueError("Payload AS24 invalido (AUTO: make/model)")

    elif vehicle_type == "X":
        if not as24_make_id:
            raise ValueError("Payload AS24 invalido (VIC: make mancante)")


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
        "vehicleType": vehicle_type,
        "offerType": "U",

        # ID AS24
        "make": as24_make_id,

        "firstRegistrationDate": first_reg,
        "mileage": mileage,


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
    if vehicle_type == "C":
        payload["model"] = as24_model_id
        if as24_model_version:
            payload["modelVersion"] = as24_model_version



    elif vehicle_type == "X":
        # AS24 richiede modelName per X
        if not as24_model_version:
            raise ValueError(
                "modelName obbligatorio per vehicleType=X (VIC)"
            )
        payload["modelName"] = as24_model_version

    
    # Transmission (AS24 enum, risolto a monte)
    if as24_transmission is not None:
        payload["transmission"] = as24_transmission

    # -----------------------------
    # Dati tecnici veicolo
    # -----------------------------

    # -----------------------------
    # Normativa Euro (AutoScout24)
    # -----------------------------
    eu_directive = auto.get("eu_emission_directive")

    if eu_directive and eu_directive != "ND":
        as24_id = AS24_EURO_MAP.get(eu_directive)
        if as24_id:
            payload["euEmissionStandard"] = as24_id

    if as24_primary_fuel_type is not None:
        payload["primaryFuelType"] = as24_primary_fuel_type

    if as24_fuel_category is not None:
        payload["fuelCategory"] = as24_fuel_category


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
        payload["lastTechnicalServiceDate"] = normalize_year_month(
            as24_last_service_date
        )

    if as24_description:
        payload["description"] = as24_description

    if as24_equipment_ids:
        payload["equipment"] = as24_equipment_ids

    if as24_has_full_service_history is not None:
        payload["hasFullServiceHistory"] = as24_has_full_service_history

    if as24_drivetrain:
        payload["drivetrain"] = as24_drivetrain

    if as24_previous_owner_count is not None:
        payload["previousOwnerCount"] = as24_previous_owner_count


    # -----------------------------
    # Cerchi in lega (AS24)
    # -----------------------------
    if (
        alloy_wheel_size is not None
        and as24_equipment_ids
        and 15 in as24_equipment_ids  # AS24: Cerchi in lega
    ):
        payload["alloyWheelSize"] = int(alloy_wheel_size)
    
    
    # -----------------------------
    # Warranty AS24 (mesi)
    # -----------------------------
    if as24_warranty_months and as24_warranty_months > 0:
        payload["warranty"] = as24_warranty_months

    # -------------------------------------------------
    # Colori / Interni / Vernice (AS24 - DB driven)
    # -------------------------------------------------
    if autoscout_attrs:
        if autoscout_attrs.get("as24_body_color_id") is not None:
            payload["bodyColor"] = autoscout_attrs["as24_body_color_id"]

        if autoscout_attrs.get("as24_upholstery_color_id") is not None:
            payload["upholsteryColor"] = autoscout_attrs["as24_upholstery_color_id"]

        if autoscout_attrs.get("as24_upholstery_type_code"):
            payload["upholsteryType"] = autoscout_attrs["as24_upholstery_type_code"]

        if autoscout_attrs.get("is_metallic") is not None:
            payload["isMetallic"] = autoscout_attrs["is_metallic"]

    # -------------------------------------------------
    # Emissioni CO2 (AS24)
    # -------------------------------------------------
    if as24_co2 is not None:
        payload["co2Emissions"] = int(as24_co2)

    # -------------------------------------------------
    # Consumi carburante (AS24)
    # -------------------------------------------------
    if (
        as24_consumo_urbano is not None
        or as24_consumo_extraurbano is not None
        or as24_consumo_medio is not None
    ):
        payload["fuelConsumption"] = {}

        if as24_consumo_urbano is not None:
            payload["fuelConsumption"]["urban"] = float(as24_consumo_urbano)

        if as24_consumo_extraurbano is not None:
            payload["fuelConsumption"]["extraUrban"] = float(as24_consumo_extraurbano)

        if as24_consumo_medio is not None:
            payload["fuelConsumption"]["combined"] = float(as24_consumo_medio)


    # -------------------------------------------------
    # Numero marce (AS24)
    # -------------------------------------------------
    if gear_count is not None:
        payload["gearCount"] = gear_count


    payload["condition"] = {
        "hadAccident": False
    }
    # Nazionalità veicolo (decisione temporanea)
    payload["countryVersion"] = "IT"


    return payload
