import re
from typing import Optional, Tuple


def match_bmw_model(
    allestimento: str,
    as24_model_names: list[str]
) -> Tuple[Optional[str], str, float]:

    if not allestimento:
        return None, "NONE", 0.0

    text = allestimento.upper().strip()
    as24_norm = {m.upper(): m for m in as24_model_names}

    # --------------------------------------------------
    # 0️⃣ ACTIVE HYBRID
    # --------------------------------------------------
    m_ah = re.match(r"^ACTIVE HYBRID\s+(X6|[357])", text)
    if m_ah:
        candidate = f"ACTIVE HYBRID {m_ah.group(1)}"
        if candidate in as24_norm:
            return as24_norm[candidate], "BMW_ACTIVE_HYBRID", 0.99

    # --------------------------------------------------
    # 1️⃣ BMW i / iX family
    # --------------------------------------------------
    m_i = re.match(r"^(I|IX)\s*(\d)?", text)
    if m_i:
        base = "IX" if m_i.group(1) == "IX" else "I"
        suffix = m_i.group(2) or ""
        candidate = f"{base}{suffix}"
        if candidate in as24_norm:
            return as24_norm[candidate], "BMW_ELECTRIC", 0.99

    # --------------------------------------------------
    # 2️⃣ BMW XM
    # --------------------------------------------------
    if text.startswith("XM"):
        if "XM" in as24_norm:
            return as24_norm["XM"], "BMW_XM", 0.99

    # --------------------------------------------------
    # 3️⃣ BMW M760 / M760Li
    # --------------------------------------------------
    if text.startswith("M 760") or text.startswith("M760"):
        if "M760" in as24_norm:
            return as24_norm["M760"], "BMW_M760", 0.99

    # --------------------------------------------------
    # 4️⃣ BMW Z3 / Z8
    # --------------------------------------------------
    if text.startswith("Z3"):
        if "Z3" in as24_norm:
            return as24_norm["Z3"], "BMW_Z3", 0.98

    if text.startswith("Z8"):
        if "Z8" in as24_norm:
            return as24_norm["Z8"], "BMW_Z8", 0.98

    # --------------------------------------------------
    # 5️⃣ NUMERO A 3 CIFRE (standard BMW)
    # es: 320d → 320, 520d → 520
    # --------------------------------------------------
    m_num = re.match(r"^(\d{3})", text)
    if m_num:
        candidate = m_num.group(1)
        if candidate in as24_norm:
            return as24_norm[candidate], "BMW_NUMBER_MODEL", 0.99

    # --------------------------------------------------
    # 6️⃣ M series standard
    # --------------------------------------------------
    m_m = re.match(r"^M\s*(\d{1,3})", text)
    if m_m:
        candidate = f"M{m_m.group(1)}"
        if candidate in as24_norm:
            return as24_norm[candidate], "BMW_M", 0.99

    # --------------------------------------------------
    # 7️⃣ X series
    # --------------------------------------------------
    m_x = re.match(r"^X\s*(\d)", text)
    if m_x:
        candidate = f"X{m_x.group(1)}"
        if candidate in as24_norm:
            return as24_norm[candidate], "BMW_X", 0.99

    # --------------------------------------------------
    # 8️⃣ Z4
    # --------------------------------------------------
    if text.startswith("Z4") and "Z4" in as24_norm:
        return as24_norm["Z4"], "BMW_Z4", 0.98

    return None, "NONE", 0.0
