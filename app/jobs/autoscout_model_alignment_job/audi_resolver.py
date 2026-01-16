import re
from typing import Tuple, Optional


def match_audi_model(
    allestimento: str,
    as24_model_names: list[str]
) -> Tuple[Optional[str], str, float]:

    if not allestimento:
        return None, "NONE", 0.0

    text = allestimento.upper().strip()
    as24_norm = {m.upper(): m for m in as24_model_names}

    # --------------------------------------------------
    # 1️⃣ RS / S (priorità alta)
    # es: RS4, S3, RS Q8
    # --------------------------------------------------
    m_perf = re.match(r"^(RS|S)\s*(Q?\d)", text)
    if m_perf:
        candidate = f"{m_perf.group(1)}{m_perf.group(2)}"
        if candidate in as24_norm:
            return as24_norm[candidate], "AUDI_PERFORMANCE", 0.99

    # --------------------------------------------------
    # 2️⃣ A / Q standard
    # es: A4, A6, Q5, Q7
    # --------------------------------------------------
    m_std = re.match(r"^(A|Q)\s*(\d)", text)
    if m_std:
        candidate = f"{m_std.group(1)}{m_std.group(2)}"
        if candidate in as24_norm:
            return as24_norm[candidate], "AUDI_STANDARD", 0.99

    # --------------------------------------------------
    # 3️⃣ TT / R8
    # --------------------------------------------------
    if text.startswith("TT") and "TT" in as24_norm:
        return as24_norm["TT"], "AUDI_TT", 0.98

    if text.startswith("R8") and "R8" in as24_norm:
        return as24_norm["R8"], "AUDI_R8", 0.98

    # --------------------------------------------------
    # 4️⃣ e-tron
    # --------------------------------------------------
    if text.startswith("E-TRON"):
        if "E-TRON" in as24_norm:
            return as24_norm["E-TRON"], "AUDI_ETRON", 0.97

    return None, "NONE", 0.0
