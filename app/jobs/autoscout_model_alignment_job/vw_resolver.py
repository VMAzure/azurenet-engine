import re
from typing import Optional, Tuple


VW_MODELS_PRIORITY = [
    "ID.BUZZ",
    "ID.5",
    "ID.4",
    "ID.3",
]

VW_MODELS_STANDARD = [
    "GOLF",
    "POLO",
    "PASSAT",
    "TIGUAN",
    "T-ROC",
    "TAIGO",
    "TOUAREG",
    "ARTEON",
    "UP",
]


def match_vw_model(
    allestimento: str,
    as24_model_names: list[str]
) -> Tuple[Optional[str], str, float]:

    if not allestimento:
        return None, "NONE", 0.0

    text = allestimento.upper().strip()
    as24_norm = {m.upper(): m for m in as24_model_names}

    # --------------------------------------------------
    # 1️⃣ ID. family (priorità massima)
    # --------------------------------------------------
    for model in VW_MODELS_PRIORITY:
        if text.startswith(model.replace(".", "")) or text.startswith(model):
            if model in as24_norm:
                return as24_norm[model], "VW_ID", 0.99

    # --------------------------------------------------
    # 2️⃣ Modelli nominali standard
    # --------------------------------------------------
    for model in VW_MODELS_STANDARD:
        if text.startswith(model):
            if model in as24_norm:
                return as24_norm[model], "VW_STANDARD", 0.99

    return None, "NONE", 0.0
