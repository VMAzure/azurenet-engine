import re
from typing import Tuple, List, Optional

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

BLACKLIST = {"quattro", "allroad", "sportback"}
ENGINE_SUFFIX_RE = r"(d|i|tdi|mhev|hybrid|e)$"

# --------------------------------------------------
# NORMALIZATION
# --------------------------------------------------

def normalize(text: str) -> str:
    return text.lower().strip()

def strip_blacklist(text: str, blacklist: set[str]) -> str:
    for b in blacklist:
        text = re.sub(rf"\b{b}\b", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def model_tokens(model: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", normalize(model))

def normalize_model_token(model: str) -> str:
    """
    Estrae un token modello confrontabile:
    - preferisce token alfanumerici con numeri
    - evita parole descrittive (sportback, touring, ecc.)
    """
    tokens = model_tokens(model)
    numeric_like = [t for t in tokens if any(c.isdigit() for c in t)]
    if numeric_like:
        return numeric_like[-1]
    return tokens[-1] if tokens else model

# --------------------------------------------------
# MATCHER
# --------------------------------------------------

def match_as24_model(
    marca: str,
    allestimento: str,
    as24_models: List[str]
) -> Tuple[Optional[str], str, float]:
    """
    Ritorna:
    - model (str | None)
    - reason (str)
    - confidence (float 0.0–1.0)
    """

    text = normalize(allestimento)
    text = strip_blacklist(text, BLACKLIST)

    matches: List[tuple[str, str]] = []

    # --------------------------------------------------
    # 1️⃣ MATCH TOKEN ISOLATO (ROBUSTO)
    # --------------------------------------------------
    for model in as24_models:
        token = normalize_model_token(model)
        if not token:
            continue

        pattern = rf"(^|[^a-z0-9]){re.escape(token)}([^a-z0-9]|$)"
        if re.search(pattern, text):
            matches.append((model, "direct_token"))

    # --------------------------------------------------
    # 2️⃣ MATCH NUMERICO CON SUFFISSO (320d → 320)
    # --------------------------------------------------
    if not matches:
        nums = re.findall(r"\b(\d{1,3})[a-z]?\b", text)
        for model in as24_models:
            model_norm = normalize(model)
            for n in nums:
                if model_norm.endswith(n):
                    matches.append((model, "numeric_suffix"))

    if not matches:
        return None, "NONE", 0.0

    # --------------------------------------------------
    # 3️⃣ DEDUP + RISOLUZIONE AMBIGUITÀ (500 vs 500X)
    # --------------------------------------------------
    unique = {m: r for m, r in matches}
    models = list(unique.keys())

    filtered: List[str] = []
    for m in models:
        m_tokens = model_tokens(m)
        if not any(
            other != m and model_tokens(other)[:len(m_tokens)] == m_tokens
            for other in models
        ):
            filtered.append(m)

    if len(filtered) == 1:
        model = filtered[0]
        reason = unique[model]
        confidence = 0.95

        # --------------------------------------------------
        # 4️⃣ BOOST CONFIDENCE SE SUFFISSO MOTORE PRESENTE
        # --------------------------------------------------
        token = normalize_model_token(model)
        if re.search(rf"{re.escape(token)}\s*{ENGINE_SUFFIX_RE}", text):
            confidence = min(confidence + 0.03, 0.99)

        return model, reason, confidence

    return None, "AMBIGUO", 0.5


# --------------------------------------------------
# DRY RUN
# --------------------------------------------------

if __name__ == "__main__":
    AS24_MODELS = {
        "BMW": ["120", "320", "325", "328", "330"],
        "Mercedes": ["A180", "C220", "E200", "S350"],
        "Audi": ["S4", "A3", "Q5", "TT", "QUATTRO"],
        "Fiat": ["500", "500X"],
        "Volvo": ["S60", "XC60"],
    }

    MNET_ALLESTIMENTI = [
        ("BMW", "BMW Serie 3 320d Touring MSport"),
        ("Mercedes", "Mercedes Classe C 220d Avantgarde"),
        ("Audi", "S4 3.0 tfsi quattro s-tronic"),
        ("Fiat", "500X 1.6 mjt Cross"),
        ("Mazda", "6 Berlina 2.3 Leather&Bose"),
    ]

    for marca, allestimento in MNET_ALLESTIMENTI:
        model, reason, confidence = match_as24_model(
            marca,
            allestimento,
            AS24_MODELS.get(marca, [])
        )

        print("--------------------------------------------------")
        print("Marca        :", marca)
        print("Allestimento :", allestimento)
        print("AS24 model   :", model)
        print("Reason       :", reason)
        print("Confidence   :", confidence)
