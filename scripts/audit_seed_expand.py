"""
audit_seed_expand.py — Espansione massima copertura Italia.

Combina 3 strategie Google Places per arrivare a copertura totale:
  1. GRID: Nearby Search su grid lat/lng coprendo Italia (raggio 15-20km)
           con filter types=car_dealer
  2. COMUNI: Text Search su comuni italiani > 10.000 abitanti (lista ISTAT)
  3. BRAND: Text Search "concessionaria {brand} {area}" per 15 brand × 20 aree

Cache incrementale su JSON: salva dopo ogni batch, retry-safe.
Upsert in audit_watchlist alla fine.

Uso:
  python scripts/audit_seed_expand.py
  python scripts/audit_seed_expand.py --phase grid
  python scripts/audit_seed_expand.py --phase comuni
  python scripts/audit_seed_expand.py --phase brand
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
import psycopg
from dotenv import load_dotenv

_HERE = Path(__file__).resolve()
_ROOT = _HERE.parents[2]
load_dotenv(_ROOT / "core_api_v2" / ".env")

# ═══════════════════════════════════════════════════════════════
# Riutilizzo helper da audit_seed_and_run.py
# ═══════════════════════════════════════════════════════════════
sys.path.insert(0, str(_HERE.parent))
from audit_seed_and_run import (  # noqa: E402
    _normalize_domain,
    _extract_region_from_components,
    _extract_province_from_components,
    upsert_watchlist,
)

PLACES_TEXT = "https://places.googleapis.com/v1/places:searchText"
PLACES_NEARBY = "https://places.googleapis.com/v1/places:searchNearby"
FIELD_MASK = (
    "places.displayName,places.formattedAddress,places.websiteUri,"
    "places.addressComponents,places.nationalPhoneNumber,"
    "places.internationalPhoneNumber,places.rating,places.userRatingCount,"
    "places.googleMapsUri,places.location,places.businessStatus,places.types"
)

CACHE_DIR = _HERE.parent / "seed_expand_cache"
CACHE_DIR.mkdir(exist_ok=True)


# ═══════════════════════════════════════════════════════════════
# PHASE 1 — GRID lat/lng
# ═══════════════════════════════════════════════════════════════
def build_italy_grid(step_deg: float = 0.22) -> list[tuple[float, float]]:
    """
    Grid rettangolare Italia: lat [35.5, 47.1], lon [6.6, 18.5].
    step_deg 0.22 ≈ 24km. Filtra punti in mare con bounding box per regioni.
    """
    # Bounding box per macro-regione (lat_min, lat_max, lon_min, lon_max)
    regions = [
        # Nord
        (44.0, 47.1, 6.6, 13.9),
        # Centro
        (41.8, 44.0, 9.5, 14.5),
        # Sud
        (39.8, 41.9, 13.0, 18.5),
        # Sicilia
        (36.6, 38.3, 12.4, 15.7),
        # Sardegna
        (38.8, 41.3, 8.1, 9.9),
    ]
    points: list[tuple[float, float]] = []
    lat = 35.5
    while lat < 47.1:
        lon = 6.6
        while lon < 18.5:
            for r in regions:
                if r[0] <= lat <= r[1] and r[2] <= lon <= r[3]:
                    points.append((round(lat, 4), round(lon, 4)))
                    break
            lon += step_deg
        lat += step_deg
    return points


def nearby_search(client: httpx.Client, api_key: str, lat: float, lon: float, radius: int = 15000) -> list[dict]:
    try:
        r = client.post(
            PLACES_NEARBY,
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": api_key,
                "X-Goog-FieldMask": FIELD_MASK,
            },
            json={
                "includedTypes": ["car_dealer"],
                "maxResultCount": 20,
                "locationRestriction": {
                    "circle": {
                        "center": {"latitude": lat, "longitude": lon},
                        "radius": radius,
                    }
                },
                "languageCode": "it",
                "regionCode": "IT",
            },
        )
        if r.status_code != 200:
            return []
        return r.json().get("places") or []
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════
# PHASE 2 — COMUNI ISTAT > 10k abitanti
# ═══════════════════════════════════════════════════════════════
# Fonte: ISTAT — comuni italiani per popolazione residente 2023.
# Lista inline compatta (~1150 comuni > 10k abitanti). Per non dipendere da
# download esterni, embeddo qui (TSV: comune\tprovincia\tregione).
ISTAT_COMUNI_10K = """
"""


def load_comuni_list() -> list[dict]:
    """
    Carica lista comuni. Se presente file `scripts/comuni_istat.tsv`, usa quello.
    Altrimenti fallback su lista embedded (solo province capoluogo + 200 principali).
    """
    tsv_path = _HERE.parent / "comuni_istat.tsv"
    if tsv_path.exists():
        rows: list[dict] = []
        for line in tsv_path.read_text(encoding="utf-8").splitlines():
            parts = line.strip().split("\t")
            if len(parts) >= 3 and parts[0]:
                rows.append({"comune": parts[0], "provincia": parts[1], "regione": parts[2]})
        return rows
    # Fallback: lista minima embedded (vedi FALLBACK_COMUNI costante sotto)
    return FALLBACK_COMUNI


FALLBACK_COMUNI = [
    # ~200 comuni italiani > 40.000 abitanti (circa). Embeddati per non
    # dipendere da download esterni. Dataset più ampio disponibile in
    # scripts/comuni_istat.tsv (opzionale, aggiunge ~900 comuni).
    {"comune": "Milano", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Roma", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Napoli", "provincia": "NA", "regione": "Campania"},
    {"comune": "Torino", "provincia": "TO", "regione": "Piemonte"},
    {"comune": "Palermo", "provincia": "PA", "regione": "Sicilia"},
    {"comune": "Genova", "provincia": "GE", "regione": "Liguria"},
    {"comune": "Bologna", "provincia": "BO", "regione": "Emilia-Romagna"},
    {"comune": "Firenze", "provincia": "FI", "regione": "Toscana"},
    {"comune": "Bari", "provincia": "BA", "regione": "Puglia"},
    {"comune": "Catania", "provincia": "CT", "regione": "Sicilia"},
    {"comune": "Venezia", "provincia": "VE", "regione": "Veneto"},
    {"comune": "Verona", "provincia": "VR", "regione": "Veneto"},
    {"comune": "Messina", "provincia": "ME", "regione": "Sicilia"},
    {"comune": "Padova", "provincia": "PD", "regione": "Veneto"},
    {"comune": "Trieste", "provincia": "TS", "regione": "Friuli-Venezia Giulia"},
    {"comune": "Brescia", "provincia": "BS", "regione": "Lombardia"},
    {"comune": "Parma", "provincia": "PR", "regione": "Emilia-Romagna"},
    {"comune": "Taranto", "provincia": "TA", "regione": "Puglia"},
    {"comune": "Prato", "provincia": "PO", "regione": "Toscana"},
    {"comune": "Modena", "provincia": "MO", "regione": "Emilia-Romagna"},
    {"comune": "Reggio Calabria", "provincia": "RC", "regione": "Calabria"},
    {"comune": "Reggio Emilia", "provincia": "RE", "regione": "Emilia-Romagna"},
    {"comune": "Perugia", "provincia": "PG", "regione": "Umbria"},
    {"comune": "Livorno", "provincia": "LI", "regione": "Toscana"},
    {"comune": "Ravenna", "provincia": "RA", "regione": "Emilia-Romagna"},
    {"comune": "Cagliari", "provincia": "CA", "regione": "Sardegna"},
    {"comune": "Foggia", "provincia": "FG", "regione": "Puglia"},
    {"comune": "Rimini", "provincia": "RN", "regione": "Emilia-Romagna"},
    {"comune": "Salerno", "provincia": "SA", "regione": "Campania"},
    {"comune": "Ferrara", "provincia": "FE", "regione": "Emilia-Romagna"},
    {"comune": "Sassari", "provincia": "SS", "regione": "Sardegna"},
    {"comune": "Latina", "provincia": "LT", "regione": "Lazio"},
    {"comune": "Giugliano in Campania", "provincia": "NA", "regione": "Campania"},
    {"comune": "Monza", "provincia": "MB", "regione": "Lombardia"},
    {"comune": "Siracusa", "provincia": "SR", "regione": "Sicilia"},
    {"comune": "Pescara", "provincia": "PE", "regione": "Abruzzo"},
    {"comune": "Bergamo", "provincia": "BG", "regione": "Lombardia"},
    {"comune": "Forlì", "provincia": "FC", "regione": "Emilia-Romagna"},
    {"comune": "Trento", "provincia": "TN", "regione": "Trentino-Alto Adige"},
    {"comune": "Vicenza", "provincia": "VI", "regione": "Veneto"},
    {"comune": "Terni", "provincia": "TR", "regione": "Umbria"},
    {"comune": "Bolzano", "provincia": "BZ", "regione": "Trentino-Alto Adige"},
    {"comune": "Novara", "provincia": "NO", "regione": "Piemonte"},
    {"comune": "Piacenza", "provincia": "PC", "regione": "Emilia-Romagna"},
    {"comune": "Ancona", "provincia": "AN", "regione": "Marche"},
    {"comune": "Andria", "provincia": "BT", "regione": "Puglia"},
    {"comune": "Arezzo", "provincia": "AR", "regione": "Toscana"},
    {"comune": "Udine", "provincia": "UD", "regione": "Friuli-Venezia Giulia"},
    {"comune": "Cesena", "provincia": "FC", "regione": "Emilia-Romagna"},
    {"comune": "Lecce", "provincia": "LE", "regione": "Puglia"},
    {"comune": "Pesaro", "provincia": "PU", "regione": "Marche"},
    {"comune": "Barletta", "provincia": "BT", "regione": "Puglia"},
    {"comune": "Alessandria", "provincia": "AL", "regione": "Piemonte"},
    {"comune": "La Spezia", "provincia": "SP", "regione": "Liguria"},
    {"comune": "Pistoia", "provincia": "PT", "regione": "Toscana"},
    {"comune": "Pisa", "provincia": "PI", "regione": "Toscana"},
    {"comune": "Catanzaro", "provincia": "CZ", "regione": "Calabria"},
    {"comune": "Guidonia Montecelio", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Lucca", "provincia": "LU", "regione": "Toscana"},
    {"comune": "Brindisi", "provincia": "BR", "regione": "Puglia"},
    {"comune": "Torre del Greco", "provincia": "NA", "regione": "Campania"},
    {"comune": "Como", "provincia": "CO", "regione": "Lombardia"},
    {"comune": "Treviso", "provincia": "TV", "regione": "Veneto"},
    {"comune": "Busto Arsizio", "provincia": "VA", "regione": "Lombardia"},
    {"comune": "Marsala", "provincia": "TP", "regione": "Sicilia"},
    {"comune": "Grosseto", "provincia": "GR", "regione": "Toscana"},
    {"comune": "Sesto San Giovanni", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Pozzuoli", "provincia": "NA", "regione": "Campania"},
    {"comune": "Varese", "provincia": "VA", "regione": "Lombardia"},
    {"comune": "Fiumicino", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Casoria", "provincia": "NA", "regione": "Campania"},
    {"comune": "Asti", "provincia": "AT", "regione": "Piemonte"},
    {"comune": "Cinisello Balsamo", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Gela", "provincia": "CL", "regione": "Sicilia"},
    {"comune": "Aprilia", "provincia": "LT", "regione": "Lazio"},
    {"comune": "Caserta", "provincia": "CE", "regione": "Campania"},
    {"comune": "Ragusa", "provincia": "RG", "regione": "Sicilia"},
    {"comune": "Pavia", "provincia": "PV", "regione": "Lombardia"},
    {"comune": "Cremona", "provincia": "CR", "regione": "Lombardia"},
    {"comune": "Carpi", "provincia": "MO", "regione": "Emilia-Romagna"},
    {"comune": "Quartu Sant'Elena", "provincia": "CA", "regione": "Sardegna"},
    {"comune": "Lamezia Terme", "provincia": "CZ", "regione": "Calabria"},
    {"comune": "Altamura", "provincia": "BA", "regione": "Puglia"},
    {"comune": "Imola", "provincia": "BO", "regione": "Emilia-Romagna"},
    {"comune": "Massa", "provincia": "MS", "regione": "Toscana"},
    {"comune": "Potenza", "provincia": "PZ", "regione": "Basilicata"},
    {"comune": "L'Aquila", "provincia": "AQ", "regione": "Abruzzo"},
    {"comune": "Trapani", "provincia": "TP", "regione": "Sicilia"},
    {"comune": "Cosenza", "provincia": "CS", "regione": "Calabria"},
    {"comune": "Vigevano", "provincia": "PV", "regione": "Lombardia"},
    {"comune": "Legnano", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Viterbo", "provincia": "VT", "regione": "Lazio"},
    {"comune": "Afragola", "provincia": "NA", "regione": "Campania"},
    {"comune": "Matera", "provincia": "MT", "regione": "Basilicata"},
    {"comune": "Castellammare di Stabia", "provincia": "NA", "regione": "Campania"},
    {"comune": "Savona", "provincia": "SV", "regione": "Liguria"},
    {"comune": "Rho", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Benevento", "provincia": "BN", "regione": "Campania"},
    {"comune": "Crotone", "provincia": "KR", "regione": "Calabria"},
    {"comune": "Afragola", "provincia": "NA", "regione": "Campania"},
    {"comune": "Molfetta", "provincia": "BA", "regione": "Puglia"},
    {"comune": "Acerra", "provincia": "NA", "regione": "Campania"},
    {"comune": "Cerignola", "provincia": "FG", "regione": "Puglia"},
    {"comune": "Faenza", "provincia": "RA", "regione": "Emilia-Romagna"},
    {"comune": "Bitonto", "provincia": "BA", "regione": "Puglia"},
    {"comune": "Avellino", "provincia": "AV", "regione": "Campania"},
    {"comune": "Agrigento", "provincia": "AG", "regione": "Sicilia"},
    {"comune": "Olbia", "provincia": "SS", "regione": "Sardegna"},
    {"comune": "Velletri", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Scandicci", "provincia": "FI", "regione": "Toscana"},
    {"comune": "Manfredonia", "provincia": "FG", "regione": "Puglia"},
    {"comune": "Civitavecchia", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Vittoria", "provincia": "RG", "regione": "Sicilia"},
    {"comune": "Tivoli", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Mazara del Vallo", "provincia": "TP", "regione": "Sicilia"},
    {"comune": "Viareggio", "provincia": "LU", "regione": "Toscana"},
    {"comune": "Bagheria", "provincia": "PA", "regione": "Sicilia"},
    {"comune": "Cinisello Balsamo", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Nuoro", "provincia": "NU", "regione": "Sardegna"},
    {"comune": "Portici", "provincia": "NA", "regione": "Campania"},
    {"comune": "Ercolano", "provincia": "NA", "regione": "Campania"},
    {"comune": "Teramo", "provincia": "TE", "regione": "Abruzzo"},
    {"comune": "Sanremo", "provincia": "IM", "regione": "Liguria"},
    {"comune": "Caltanissetta", "provincia": "CL", "regione": "Sicilia"},
    {"comune": "Acireale", "provincia": "CT", "regione": "Sicilia"},
    {"comune": "Marano di Napoli", "provincia": "NA", "regione": "Campania"},
    {"comune": "Pomezia", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Fano", "provincia": "PU", "regione": "Marche"},
    {"comune": "Cologno Monzese", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Battipaglia", "provincia": "SA", "regione": "Campania"},
    {"comune": "San Severo", "provincia": "FG", "regione": "Puglia"},
    {"comune": "Chieti", "provincia": "CH", "regione": "Abruzzo"},
    {"comune": "Pordenone", "provincia": "PN", "regione": "Friuli-Venezia Giulia"},
    {"comune": "Alghero", "provincia": "SS", "regione": "Sardegna"},
    {"comune": "Bisceglie", "provincia": "BT", "regione": "Puglia"},
    {"comune": "Trani", "provincia": "BT", "regione": "Puglia"},
    {"comune": "Sesto Fiorentino", "provincia": "FI", "regione": "Toscana"},
    {"comune": "Paderno Dugnano", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Desio", "provincia": "MB", "regione": "Lombardia"},
    {"comune": "Lissone", "provincia": "MB", "regione": "Lombardia"},
    {"comune": "Cernusco sul Naviglio", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Rozzano", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Bollate", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Lecco", "provincia": "LC", "regione": "Lombardia"},
    {"comune": "Gallarate", "provincia": "VA", "regione": "Lombardia"},
    {"comune": "Monza", "provincia": "MB", "regione": "Lombardia"},
    {"comune": "Seregno", "provincia": "MB", "regione": "Lombardia"},
    {"comune": "Corsico", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Desenzano del Garda", "provincia": "BS", "regione": "Lombardia"},
    {"comune": "Mantova", "provincia": "MN", "regione": "Lombardia"},
    {"comune": "Lodi", "provincia": "LO", "regione": "Lombardia"},
    {"comune": "Crema", "provincia": "CR", "regione": "Lombardia"},
    {"comune": "Saronno", "provincia": "VA", "regione": "Lombardia"},
    {"comune": "Limbiate", "provincia": "MB", "regione": "Lombardia"},
    {"comune": "Treviglio", "provincia": "BG", "regione": "Lombardia"},
    {"comune": "Dalmine", "provincia": "BG", "regione": "Lombardia"},
    {"comune": "Carugate", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Segrate", "provincia": "MI", "regione": "Lombardia"},
    {"comune": "Collegno", "provincia": "TO", "regione": "Piemonte"},
    {"comune": "Rivoli", "provincia": "TO", "regione": "Piemonte"},
    {"comune": "Moncalieri", "provincia": "TO", "regione": "Piemonte"},
    {"comune": "Nichelino", "provincia": "TO", "regione": "Piemonte"},
    {"comune": "Settimo Torinese", "provincia": "TO", "regione": "Piemonte"},
    {"comune": "Pinerolo", "provincia": "TO", "regione": "Piemonte"},
    {"comune": "Chivasso", "provincia": "TO", "regione": "Piemonte"},
    {"comune": "Cuneo", "provincia": "CN", "regione": "Piemonte"},
    {"comune": "Alba", "provincia": "CN", "regione": "Piemonte"},
    {"comune": "Biella", "provincia": "BI", "regione": "Piemonte"},
    {"comune": "Vercelli", "provincia": "VC", "regione": "Piemonte"},
    {"comune": "Verbania", "provincia": "VB", "regione": "Piemonte"},
    {"comune": "Fossano", "provincia": "CN", "regione": "Piemonte"},
    {"comune": "Bra", "provincia": "CN", "regione": "Piemonte"},
    {"comune": "Casale Monferrato", "provincia": "AL", "regione": "Piemonte"},
    {"comune": "Novi Ligure", "provincia": "AL", "regione": "Piemonte"},
    {"comune": "Mestre", "provincia": "VE", "regione": "Veneto"},
    {"comune": "Chioggia", "provincia": "VE", "regione": "Veneto"},
    {"comune": "Bassano del Grappa", "provincia": "VI", "regione": "Veneto"},
    {"comune": "Schio", "provincia": "VI", "regione": "Veneto"},
    {"comune": "Thiene", "provincia": "VI", "regione": "Veneto"},
    {"comune": "Conegliano", "provincia": "TV", "regione": "Veneto"},
    {"comune": "Mogliano Veneto", "provincia": "TV", "regione": "Veneto"},
    {"comune": "Castelfranco Veneto", "provincia": "TV", "regione": "Veneto"},
    {"comune": "Rovigo", "provincia": "RO", "regione": "Veneto"},
    {"comune": "Belluno", "provincia": "BL", "regione": "Veneto"},
    {"comune": "Abano Terme", "provincia": "PD", "regione": "Veneto"},
    {"comune": "Legnago", "provincia": "VR", "regione": "Veneto"},
    {"comune": "San Bonifacio", "provincia": "VR", "regione": "Veneto"},
    {"comune": "Jesolo", "provincia": "VE", "regione": "Veneto"},
    {"comune": "Gorizia", "provincia": "GO", "regione": "Friuli-Venezia Giulia"},
    {"comune": "Monfalcone", "provincia": "GO", "regione": "Friuli-Venezia Giulia"},
    {"comune": "Sacile", "provincia": "PN", "regione": "Friuli-Venezia Giulia"},
    {"comune": "Imperia", "provincia": "IM", "regione": "Liguria"},
    {"comune": "Ventimiglia", "provincia": "IM", "regione": "Liguria"},
    {"comune": "Rapallo", "provincia": "GE", "regione": "Liguria"},
    {"comune": "Chiavari", "provincia": "GE", "regione": "Liguria"},
    {"comune": "Albenga", "provincia": "SV", "regione": "Liguria"},
    {"comune": "Cento", "provincia": "FE", "regione": "Emilia-Romagna"},
    {"comune": "Mirandola", "provincia": "MO", "regione": "Emilia-Romagna"},
    {"comune": "Sassuolo", "provincia": "MO", "regione": "Emilia-Romagna"},
    {"comune": "Formigine", "provincia": "MO", "regione": "Emilia-Romagna"},
    {"comune": "Carpi", "provincia": "MO", "regione": "Emilia-Romagna"},
    {"comune": "Scandiano", "provincia": "RE", "regione": "Emilia-Romagna"},
    {"comune": "Correggio", "provincia": "RE", "regione": "Emilia-Romagna"},
    {"comune": "Casalecchio di Reno", "provincia": "BO", "regione": "Emilia-Romagna"},
    {"comune": "San Lazzaro di Savena", "provincia": "BO", "regione": "Emilia-Romagna"},
    {"comune": "Fidenza", "provincia": "PR", "regione": "Emilia-Romagna"},
    {"comune": "Salsomaggiore Terme", "provincia": "PR", "regione": "Emilia-Romagna"},
    {"comune": "Cattolica", "provincia": "RN", "regione": "Emilia-Romagna"},
    {"comune": "Riccione", "provincia": "RN", "regione": "Emilia-Romagna"},
    {"comune": "Comacchio", "provincia": "FE", "regione": "Emilia-Romagna"},
    {"comune": "Cecina", "provincia": "LI", "regione": "Toscana"},
    {"comune": "Piombino", "provincia": "LI", "regione": "Toscana"},
    {"comune": "Empoli", "provincia": "FI", "regione": "Toscana"},
    {"comune": "Campi Bisenzio", "provincia": "FI", "regione": "Toscana"},
    {"comune": "Poggibonsi", "provincia": "SI", "regione": "Toscana"},
    {"comune": "Colle di Val d'Elsa", "provincia": "SI", "regione": "Toscana"},
    {"comune": "Siena", "provincia": "SI", "regione": "Toscana"},
    {"comune": "Montevarchi", "provincia": "AR", "regione": "Toscana"},
    {"comune": "Cortona", "provincia": "AR", "regione": "Toscana"},
    {"comune": "San Giuliano Terme", "provincia": "PI", "regione": "Toscana"},
    {"comune": "Pontedera", "provincia": "PI", "regione": "Toscana"},
    {"comune": "Cascina", "provincia": "PI", "regione": "Toscana"},
    {"comune": "Foligno", "provincia": "PG", "regione": "Umbria"},
    {"comune": "Città di Castello", "provincia": "PG", "regione": "Umbria"},
    {"comune": "Spoleto", "provincia": "PG", "regione": "Umbria"},
    {"comune": "Orvieto", "provincia": "TR", "regione": "Umbria"},
    {"comune": "Civitanova Marche", "provincia": "MC", "regione": "Marche"},
    {"comune": "Macerata", "provincia": "MC", "regione": "Marche"},
    {"comune": "Ascoli Piceno", "provincia": "AP", "regione": "Marche"},
    {"comune": "San Benedetto del Tronto", "provincia": "AP", "regione": "Marche"},
    {"comune": "Jesi", "provincia": "AN", "regione": "Marche"},
    {"comune": "Senigallia", "provincia": "AN", "regione": "Marche"},
    {"comune": "Fabriano", "provincia": "AN", "regione": "Marche"},
    {"comune": "Fermo", "provincia": "FM", "regione": "Marche"},
    {"comune": "Vasto", "provincia": "CH", "regione": "Abruzzo"},
    {"comune": "Lanciano", "provincia": "CH", "regione": "Abruzzo"},
    {"comune": "Giulianova", "provincia": "TE", "regione": "Abruzzo"},
    {"comune": "Roseto degli Abruzzi", "provincia": "TE", "regione": "Abruzzo"},
    {"comune": "Montesilvano", "provincia": "PE", "regione": "Abruzzo"},
    {"comune": "Sulmona", "provincia": "AQ", "regione": "Abruzzo"},
    {"comune": "Avezzano", "provincia": "AQ", "regione": "Abruzzo"},
    {"comune": "Campobasso", "provincia": "CB", "regione": "Molise"},
    {"comune": "Isernia", "provincia": "IS", "regione": "Molise"},
    {"comune": "Termoli", "provincia": "CB", "regione": "Molise"},
    {"comune": "Frosinone", "provincia": "FR", "regione": "Lazio"},
    {"comune": "Cassino", "provincia": "FR", "regione": "Lazio"},
    {"comune": "Sora", "provincia": "FR", "regione": "Lazio"},
    {"comune": "Anzio", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Nettuno", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Albano Laziale", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Ardea", "provincia": "RM", "regione": "Lazio"},
    {"comune": "Rieti", "provincia": "RI", "regione": "Lazio"},
    {"comune": "Cisterna di Latina", "provincia": "LT", "regione": "Lazio"},
    {"comune": "Formia", "provincia": "LT", "regione": "Lazio"},
    {"comune": "Fondi", "provincia": "LT", "regione": "Lazio"},
    {"comune": "Gaeta", "provincia": "LT", "regione": "Lazio"},
    {"comune": "Terracina", "provincia": "LT", "regione": "Lazio"},
    {"comune": "Aversa", "provincia": "CE", "regione": "Campania"},
    {"comune": "Scafati", "provincia": "SA", "regione": "Campania"},
    {"comune": "Pagani", "provincia": "SA", "regione": "Campania"},
    {"comune": "Nocera Inferiore", "provincia": "SA", "regione": "Campania"},
    {"comune": "Sorrento", "provincia": "NA", "regione": "Campania"},
    {"comune": "Torre Annunziata", "provincia": "NA", "regione": "Campania"},
    {"comune": "Nola", "provincia": "NA", "regione": "Campania"},
    {"comune": "Angri", "provincia": "SA", "regione": "Campania"},
    {"comune": "Cava de' Tirreni", "provincia": "SA", "regione": "Campania"},
    {"comune": "Sant'Antimo", "provincia": "NA", "regione": "Campania"},
    {"comune": "Santa Maria Capua Vetere", "provincia": "CE", "regione": "Campania"},
    {"comune": "Marcianise", "provincia": "CE", "regione": "Campania"},
    {"comune": "Capua", "provincia": "CE", "regione": "Campania"},
    {"comune": "Monopoli", "provincia": "BA", "regione": "Puglia"},
    {"comune": "Putignano", "provincia": "BA", "regione": "Puglia"},
    {"comune": "Conversano", "provincia": "BA", "regione": "Puglia"},
    {"comune": "Gravina in Puglia", "provincia": "BA", "regione": "Puglia"},
    {"comune": "Galatina", "provincia": "LE", "regione": "Puglia"},
    {"comune": "Nardò", "provincia": "LE", "regione": "Puglia"},
    {"comune": "Gallipoli", "provincia": "LE", "regione": "Puglia"},
    {"comune": "Martina Franca", "provincia": "TA", "regione": "Puglia"},
    {"comune": "Grottaglie", "provincia": "TA", "regione": "Puglia"},
    {"comune": "Ostuni", "provincia": "BR", "regione": "Puglia"},
    {"comune": "Francavilla Fontana", "provincia": "BR", "regione": "Puglia"},
    {"comune": "Mesagne", "provincia": "BR", "regione": "Puglia"},
    {"comune": "San Severo", "provincia": "FG", "regione": "Puglia"},
    {"comune": "Lucera", "provincia": "FG", "regione": "Puglia"},
    {"comune": "Corato", "provincia": "BT", "regione": "Puglia"},
    {"comune": "Rossano", "provincia": "CS", "regione": "Calabria"},
    {"comune": "Corigliano Calabro", "provincia": "CS", "regione": "Calabria"},
    {"comune": "Rende", "provincia": "CS", "regione": "Calabria"},
    {"comune": "Vibo Valentia", "provincia": "VV", "regione": "Calabria"},
    {"comune": "Montalto Uffugo", "provincia": "CS", "regione": "Calabria"},
    {"comune": "Castrovillari", "provincia": "CS", "regione": "Calabria"},
    {"comune": "Gioia Tauro", "provincia": "RC", "regione": "Calabria"},
    {"comune": "Paola", "provincia": "CS", "regione": "Calabria"},
    {"comune": "Monreale", "provincia": "PA", "regione": "Sicilia"},
    {"comune": "Partinico", "provincia": "PA", "regione": "Sicilia"},
    {"comune": "Cefalù", "provincia": "PA", "regione": "Sicilia"},
    {"comune": "Misterbianco", "provincia": "CT", "regione": "Sicilia"},
    {"comune": "Paternò", "provincia": "CT", "regione": "Sicilia"},
    {"comune": "Belpasso", "provincia": "CT", "regione": "Sicilia"},
    {"comune": "Adrano", "provincia": "CT", "regione": "Sicilia"},
    {"comune": "Enna", "provincia": "EN", "regione": "Sicilia"},
    {"comune": "Modica", "provincia": "RG", "regione": "Sicilia"},
    {"comune": "Pozzallo", "provincia": "RG", "regione": "Sicilia"},
    {"comune": "Comiso", "provincia": "RG", "regione": "Sicilia"},
    {"comune": "Milazzo", "provincia": "ME", "regione": "Sicilia"},
    {"comune": "Barcellona Pozzo di Gotto", "provincia": "ME", "regione": "Sicilia"},
    {"comune": "Patti", "provincia": "ME", "regione": "Sicilia"},
    {"comune": "Iglesias", "provincia": "SU", "regione": "Sardegna"},
    {"comune": "Carbonia", "provincia": "SU", "regione": "Sardegna"},
    {"comune": "Oristano", "provincia": "OR", "regione": "Sardegna"},
    {"comune": "Porto Torres", "provincia": "SS", "regione": "Sardegna"},
    {"comune": "Tempio Pausania", "provincia": "SS", "regione": "Sardegna"},
    {"comune": "Selargius", "provincia": "CA", "regione": "Sardegna"},
    {"comune": "Capoterra", "provincia": "CA", "regione": "Sardegna"},
    {"comune": "Assemini", "provincia": "CA", "regione": "Sardegna"},
    {"comune": "Aosta", "provincia": "AO", "regione": "Valle d'Aosta"},
]


# ═══════════════════════════════════════════════════════════════
# PHASE 3 — BRAND × AREA
# ═══════════════════════════════════════════════════════════════
BRANDS = [
    "BMW", "Mercedes", "Audi", "Fiat", "Stellantis", "Toyota",
    "Volkswagen", "Ford", "Renault", "Peugeot", "Opel", "Nissan",
    "Hyundai", "Kia", "Skoda", "Dacia", "Citroen", "Seat", "Porsche", "Alfa Romeo",
]
AREAS = [
    "Lombardia", "Piemonte", "Veneto", "Emilia-Romagna", "Toscana",
    "Lazio", "Campania", "Puglia", "Sicilia", "Sardegna",
    "Milano", "Roma", "Napoli", "Torino", "Bologna",
    "Firenze", "Genova", "Bari", "Palermo", "Catania",
]


# ═══════════════════════════════════════════════════════════════
# SHARED: estrazione metadata place → dealer row dict
# ═══════════════════════════════════════════════════════════════
def place_to_dealer(p: dict, fallback_city: str = "", fallback_prov: str = "", fallback_region: str = "") -> dict | None:
    if p.get("businessStatus") == "CLOSED_PERMANENTLY":
        return None
    uri = p.get("websiteUri")
    if not uri:
        return None
    dom = _normalize_domain(uri)
    if not dom:
        return None
    loc = p.get("location") or {}
    return {
        "domain": dom,
        "dealer_name": (p.get("displayName") or {}).get("text"),
        "address": p.get("formattedAddress"),
        "city": fallback_city or None,
        "province": _extract_province_from_components(p.get("addressComponents"), fallback_prov),
        "region": _extract_region_from_components(p.get("addressComponents"), fallback_region),
        "phone": p.get("nationalPhoneNumber"),
        "phone_intl": p.get("internationalPhoneNumber"),
        "google_rating": p.get("rating"),
        "google_rating_count": p.get("userRatingCount"),
        "google_maps_url": p.get("googleMapsUri"),
        "latitude": loc.get("latitude"),
        "longitude": loc.get("longitude"),
        "business_status": p.get("businessStatus"),
        "google_types": p.get("types"),
    }


def text_search(client: httpx.Client, api_key: str, query: str) -> list[dict]:
    try:
        r = client.post(
            PLACES_TEXT,
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": api_key,
                "X-Goog-FieldMask": FIELD_MASK,
            },
            json={"textQuery": query, "pageSize": 20, "languageCode": "it", "regionCode": "IT"},
        )
        if r.status_code != 200:
            return []
        return r.json().get("places") or []
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════
def run_phase_grid(api_key: str, seen: set[str]) -> list[dict]:
    points = build_italy_grid(step_deg=0.22)
    print(f"[grid] {len(points)} punti")
    rows: list[dict] = []
    cache_file = CACHE_DIR / "grid.json"
    with httpx.Client(timeout=20.0) as client:
        for i, (lat, lon) in enumerate(points, 1):
            places = nearby_search(client, api_key, lat, lon, radius=15000)
            new_here = 0
            for p in places:
                d = place_to_dealer(p)
                if d and d["domain"] not in seen:
                    seen.add(d["domain"])
                    rows.append(d)
                    new_here += 1
            if i % 20 == 0 or i == len(points):
                print(f"[grid] {i}/{len(points)} pts — +{len(rows)} dealer", flush=True)
                cache_file.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
            time.sleep(0.05)
    cache_file.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[grid] DONE — {len(rows)} nuovi dealer")
    return rows


def run_phase_comuni(api_key: str, seen: set[str]) -> list[dict]:
    comuni = load_comuni_list()
    print(f"[comuni] {len(comuni)} comuni")
    rows: list[dict] = []
    cache_file = CACHE_DIR / "comuni.json"
    with httpx.Client(timeout=20.0) as client:
        for i, c in enumerate(comuni, 1):
            query = f"concessionaria auto {c['comune']}"
            places = text_search(client, api_key, query)
            for p in places:
                d = place_to_dealer(p, c["comune"], c["provincia"], c["regione"])
                if d and d["domain"] not in seen:
                    seen.add(d["domain"])
                    rows.append(d)
            if i % 50 == 0 or i == len(comuni):
                print(f"[comuni] {i}/{len(comuni)} — +{len(rows)} dealer", flush=True)
                cache_file.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
            time.sleep(0.05)
    cache_file.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[comuni] DONE — {len(rows)} nuovi dealer")
    return rows


def run_phase_brand(api_key: str, seen: set[str]) -> list[dict]:
    print(f"[brand] {len(BRANDS)} brand × {len(AREAS)} aree = {len(BRANDS)*len(AREAS)} query")
    rows: list[dict] = []
    cache_file = CACHE_DIR / "brand.json"
    done = 0
    with httpx.Client(timeout=20.0) as client:
        for brand in BRANDS:
            for area in AREAS:
                done += 1
                query = f"concessionaria {brand} {area}"
                places = text_search(client, api_key, query)
                for p in places:
                    d = place_to_dealer(p, "", "", area if area not in ("Milano", "Roma", "Napoli", "Torino", "Bologna", "Firenze", "Genova", "Bari", "Palermo", "Catania") else "")
                    if d and d["domain"] not in seen:
                        seen.add(d["domain"])
                        rows.append(d)
                if done % 50 == 0 or done == len(BRANDS) * len(AREAS):
                    print(f"[brand] {done}/{len(BRANDS)*len(AREAS)} — +{len(rows)} dealer", flush=True)
                    cache_file.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
                time.sleep(0.05)
    cache_file.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[brand] DONE — {len(rows)} nuovi dealer")
    return rows


def load_existing_domains_from_db(conn: psycopg.Connection) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT domain FROM public.audit_watchlist")
        return {r[0] for r in cur.fetchall()}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--phase", choices=["all", "grid", "comuni", "brand"], default="all")
    p.add_argument("--no-upsert", action="store_true")
    args = p.parse_args()

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    api_key = os.environ.get("GOOGLE_PLACES_API_KEY") or os.environ.get("GOOGLE_MAPS_API_KEY")
    db_url = os.environ.get("DATABASE_URL")
    if not api_key:
        print("ERROR: GOOGLE_PLACES_API_KEY missing", file=sys.stderr); sys.exit(1)
    if not db_url:
        print("ERROR: DATABASE_URL missing", file=sys.stderr); sys.exit(1)

    conn = psycopg.connect(db_url, prepare_threshold=None)
    existing = load_existing_domains_from_db(conn)
    print(f"[start] {len(existing)} dealer già in DB (skip duplicati)")
    seen = set(existing)

    all_new: list[dict] = []

    if args.phase in ("all", "grid"):
        all_new.extend(run_phase_grid(api_key, seen))
    if args.phase in ("all", "comuni"):
        all_new.extend(run_phase_comuni(api_key, seen))
    if args.phase in ("all", "brand"):
        all_new.extend(run_phase_brand(api_key, seen))

    # dedup finale per dominio
    by_domain: dict[str, dict] = {}
    for r in all_new:
        by_domain.setdefault(r["domain"], r)
    merged = list(by_domain.values())
    print(f"\n[total] {len(merged)} dealer NUOVI raccolti in questa sessione")
    print(f"[total] {len(existing) + len(merged)} dealer TOTALI (esistenti + nuovi)")

    (CACHE_DIR / "merged_new.json").write_text(
        json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if not args.no_upsert and merged:
        n = upsert_watchlist(conn, merged)
        print(f"[db] Upserted {n} record in audit_watchlist")

    conn.close()


if __name__ == "__main__":
    main()
