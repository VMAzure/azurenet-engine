import re
from typing import Optional, Tuple

# Ordine = priorità (prima i modelli “ambigui” o composti)
RENAULT_MODELS_PRIORITY = [
    "MEGANE E-TECH",
    "MEGANE ETECH",
    "GRAND SCENIC",
    "SCENIC",
    "KADJAR",
    "AUSTRAL",
    "ARKANA",
    "CAPTUR",
    "CLIO",
    "TWINGO",
    "ZOE",
    "ESPACE",
    "KANGOO",
    "TRAFIC",
]

def match_renault_model(
    allestimento: str,
    as24_model_names: list[str]
) -> Tuple[Optional[str], str, float]:

    if not allestimento:
        return None, "NONE", 0.0

    text = allestimento.upper().strip()
    as24_norm = {m.upper(): m for m in as24_model_names}

    # 1️⃣ Modelli composti / prioritari
    for model in RENAULT_MODELS_PRIORITY:
        if text.startswith(model):
            if model in as24_norm:
                return as24_norm[model], "RENAULT_STANDARD", 0.99

    # 2️⃣ Fallback: prima parola nominale
    first_word = text.split(" ")[0]
    if first_word in as24_norm:
        return as24_norm[first_word], "RENAULT_FALLBACK", 0.97

    return None, "NONE", 0.0
