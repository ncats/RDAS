#!/usr/bin/env python3
"""
Affiliation Location Extraction (authors -> affiliation_node.csv)

What this script does
- Reads Authors CSV (author_id, affiliation[]) and extracts location fields from affiliation text.
- Produces affiliation_node.csv with: author_id, Aff_city, Aff_state, Aff_country, Aff_FIPS.
- For U.S. rows, computes county FIPS using uscities.csv (city + state).
- Skips inducing values: if a location cannot be confidently derived, fields are left empty.

How locations are extracted (no rule-based fallback)
- Uses spaCy NER (en_core_web_sm) to detect location entities (GPE/LOC).
- Country detection via pycountry + geonamescache; normalized to official names (e.g., United States).
- City validation against gazetteers:
  - U.S.: city must be in uscities.csv (case/diacritics normalized).
  - Non-U.S.: city must be in geonamescache for the detected country (diacritics normalized).
- If NER found at least one location entity but none validated as a city, the last GPE/LOC span is used as city.
- Otherwise the city/country/state remain empty.

Ranking when multiple affiliation strings exist per author
1) Prefer records with valid U.S. FIPS
2) Then records with a detected country
3) Then any record with city or state signal

Key dependencies and data
- spaCy (model: en_core_web_sm) for NER
- geonamescache for country/city gazetteers
- pycountry for country normalization/lookup
- Unidecode for diacritic-insensitive matching
- pandas for CSV I/O and processing
- uscities.csv (columns: city, state_name, state_id, county_fips) for FIPS resolution
"""
import argparse
import csv
import json
import os
import re
from typing import Dict, Tuple, Optional, List

import pandas as pd
import spacy
try:
    import geonamescache  # type: ignore
    import pycountry  # type: ignore
    from unidecode import unidecode  # type: ignore
except Exception as e:
    raise RuntimeError(
        "Required packages missing. Please install: `pip install spacy geonamescache pycountry pandas Unidecode` "
        "and download spaCy model: `python -m spacy download en_core_web_sm`"
    ) from e


USCITIES_DEFAULT = "data/external/uscities.csv"
AUTHORS_CSV_DEFAULT = "csv_files/authors_GARD_0006233.csv"
OUTPUT_DIR_DEFAULT = "csv_files"
OUTPUT_FILENAME = "affiliation_node.csv"

US_STATES = [
    ("AL", "Alabama"),
    ("AK", "Alaska"),
    ("AZ", "Arizona"),
    ("AR", "Arkansas"),
    ("CA", "California"),
    ("CO", "Colorado"),
    ("CT", "Connecticut"),
    ("DE", "Delaware"),
    ("FL", "Florida"),
    ("GA", "Georgia"),
    ("HI", "Hawaii"),
    ("ID", "Idaho"),
    ("IL", "Illinois"),
    ("IN", "Indiana"),
    ("IA", "Iowa"),
    ("KS", "Kansas"),
    ("KY", "Kentucky"),
    ("LA", "Louisiana"),
    ("ME", "Maine"),
    ("MD", "Maryland"),
    ("MA", "Massachusetts"),
    ("MI", "Michigan"),
    ("MN", "Minnesota"),
    ("MS", "Mississippi"),
    ("MO", "Missouri"),
    ("MT", "Montana"),
    ("NE", "Nebraska"),
    ("NV", "Nevada"),
    ("NH", "New Hampshire"),
    ("NJ", "New Jersey"),
    ("NM", "New Mexico"),
    ("NY", "New York"),
    ("NC", "North Carolina"),
    ("ND", "North Dakota"),
    ("OH", "Ohio"),
    ("OK", "Oklahoma"),
    ("OR", "Oregon"),
    ("PA", "Pennsylvania"),
    ("RI", "Rhode Island"),
    ("SC", "South Carolina"),
    ("SD", "South Dakota"),
    ("TN", "Tennessee"),
    ("TX", "Texas"),
    ("UT", "Utah"),
    ("VT", "Vermont"),
    ("VA", "Virginia"),
    ("WA", "Washington"),
    ("WV", "West Virginia"),
    ("WI", "Wisconsin"),
    ("WY", "Wyoming"),
]
STATE_ABBR_TO_FULL = {abbr.lower(): full for abbr, full in US_STATES}
STATE_FULL_TO_FULL = {full.lower(): full for _, full in US_STATES}
STATE_TOKENS = set(list(STATE_ABBR_TO_FULL.keys()) + list(STATE_FULL_TO_FULL.keys()))

_NLP = spacy.load("en_core_web_sm")
_GC = geonamescache.GeonamesCache()
_COUNTRY_NAME_TO_CODE = {cdata["name"].lower(): code for code, cdata in _GC.get_countries().items()}
_COUNTRY_ALT_TO_CODE = {}
for code, cdata in _GC.get_countries().items():
    for alt in cdata.get("alternatenames", []):
        _COUNTRY_ALT_TO_CODE[alt.lower()] = code
_COUNTRY_LOOKUP_CACHE: Dict[str, Optional[str]] = {}
_COUNTRY_CITIES_CACHE: Dict[str, set] = {}
_GLOBAL_CITY_ASCII_SET: set = set()

def _build_global_city_ascii_set(min_population: int = 100000) -> None:
    names = set()
    for _, c in _GC.get_cities().items():
        pop_raw = c.get("population")
        try:
            pop = int(pop_raw) if pop_raw not in (None, "") else 0
        except Exception:
            pop = 0
        if pop < min_population:
            continue
        def add_name(n: Optional[str]) -> None:
            if not n:
                return
            n1 = n.strip()
            if not n1:
                return
            names.add(unidecode(n1).lower())
        add_name(c.get("name"))
        for alt in c.get("alternatenames") or []:
            add_name(alt)
    _GLOBAL_CITY_ASCII_SET.clear()
    _GLOBAL_CITY_ASCII_SET.update(names)


def load_fips_map(uscities_path: str) -> Tuple[Dict[Tuple[str, str], str], Dict[str, str], Dict[str, set]]:
    """
    Returns:
      - fips_by_city_state[(city_lower, state_full_lower)] -> county_fips (5-char)
      - fips_by_city_only[city_lower] -> county_fips (last seen; ambiguous cities may collide)
      - cities_by_state_full[state_full_lower] -> set of city_lower
    """
    df = pd.read_csv(uscities_path, dtype=str, keep_default_na=False)
    needed_cols = {"city", "state_name", "state_id", "county_fips"}
    missing = needed_cols - set(df.columns)
    if missing:
        raise RuntimeError(f"uscities.csv is missing columns: {missing}")

    fips_by_city_state: Dict[Tuple[str, str], str] = {}
    fips_by_city_only: Dict[str, str] = {}
    cities_by_state: Dict[str, set] = {}

    for _, row in df.iterrows():
        city = str(row["city"]).strip().lower()
        state_full = str(row["state_name"]).strip()
        state_full_lower = state_full.lower()
        county_fips = str(row["county_fips"]).zfill(5) if row["county_fips"] else ""

        if city and state_full_lower and county_fips:
            fips_by_city_state[(city, state_full_lower)] = county_fips
            fips_by_city_only[city] = county_fips
            cities_by_state.setdefault(state_full_lower, set()).add(city)

    return fips_by_city_state, fips_by_city_only, cities_by_state


def normalize_text(s: str) -> str:
    # remove emails
    s = re.sub(r"\S+@\S+", " ", s)
    # collapse spaces
    s = " ".join(s.split())
    return s


def state_to_full(state_token: str) -> Optional[str]:
    st = state_token.strip().lower()
    if st in STATE_ABBR_TO_FULL:
        return STATE_ABBR_TO_FULL[st]
    if st in STATE_FULL_TO_FULL:
        return STATE_FULL_TO_FULL[st]
    return None


def find_fips_for_city_state(
    city: Optional[str],
    state: Optional[str],
    fips_by_city_state: Dict[Tuple[str, str], str],
    fips_by_city_only: Dict[str, str],
) -> str:
    if not city:
        return ""
    city_l = city.strip().lower()
    if state:
        st_full = state_to_full(state)
        if st_full:
            fips = fips_by_city_state.get((city_l, st_full.lower()))
            if fips:
                return fips
    # fallback: city only (ambiguous)
    return fips_by_city_only.get(city_l, "")

def _lookup_country_code(name: str) -> Optional[str]:
    key = name.strip().lower()
    if not key:
        return None
    if key in _COUNTRY_LOOKUP_CACHE:
        return _COUNTRY_LOOKUP_CACHE[key]
    # pycountry lookup first (handles various forms)
    try:
        c = pycountry.countries.lookup(name)
        _COUNTRY_LOOKUP_CACHE[key] = c.alpha_2
        return c.alpha_2
    except Exception:
        pass
    # geonamescache name/altname fallback
    if key in _COUNTRY_NAME_TO_CODE:
        _COUNTRY_LOOKUP_CACHE[key] = _COUNTRY_NAME_TO_CODE[key]
        return _COUNTRY_NAME_TO_CODE[key]
    if key in _COUNTRY_ALT_TO_CODE:
        _COUNTRY_LOOKUP_CACHE[key] = _COUNTRY_ALT_TO_CODE[key]
        return _COUNTRY_ALT_TO_CODE[key]
    _COUNTRY_LOOKUP_CACHE[key] = None
    return None


def _country_code_to_name(code: str) -> Optional[str]:
    if not code:
        return None
    try:
        c = pycountry.countries.get(alpha_2=code.upper())
        return c.name if c else None
    except Exception:
        # fallback to geonamescache
        countries = _GC.get_countries()
        data = countries.get(code.upper())
        return data["name"] if data else None


def _cities_for_country(code: str) -> set:
    code = (code or "").upper()
    if not code:
        return set()
    if code in _COUNTRY_CITIES_CACHE:
        return _COUNTRY_CITIES_CACHE[code]
    cities = set()
    for _, c in _GC.get_cities().items():
        if c.get("countrycode") == code:
            name = str(c.get("name") or "").strip()
            if name:
                cities.add(name.lower())
                cities.add(unidecode(name).lower())
            for alt in c.get("alternatenames", []) or []:
                alt = str(alt or "").strip()
                if alt:
                    cities.add(alt.lower())
                    cities.add(unidecode(alt).lower())
    _COUNTRY_CITIES_CACHE[code] = cities
    return cities


def extract_with_spacy_gazetteer(
    aff: str,
    us_city_names: set
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Use spaCy NER to find country/state/city and validate cities against gazetteers.
    - Country: pycountry/geonamescache
    - State: US states only (for FIPS)
    - City: for US, must be in us_city_names; for non-US, must be in geonames cities for that country
    """
    doc = _NLP(aff)
    ent_texts = [e.text.strip() for e in doc.ents if e.label_ in ("GPE", "LOC")]

    # Country detection
    country_code: Optional[str] = None
    for text in ent_texts:
        cc = _lookup_country_code(text)
        if cc:
            country_code = cc
    # State detection (US only)
    state: Optional[str] = None
    for text in ent_texts:
        t = text.strip()
        st_full = state_to_full(t)
        if st_full:
            state = st_full
    # If we have a US state but no explicit country, assume United States
    if state and not country_code:
        country_code = "US"

    # City detection
    city: Optional[str] = None
    if country_code == "US":
        for text in reversed(ent_texts):
            name = text.strip().lower()
            if name in us_city_names:
                city = text.strip()
                break
    elif country_code:
        allowed = _cities_for_country(country_code)
        if allowed:
            for text in reversed(ent_texts):
                name = text.strip().lower()
                if name in allowed:
                    city = text.strip()
                    break
    # If NER found a location token but didn't match allowed sets, keep the last token as city (no induction)
    if city is None and ent_texts:
        city = ent_texts[-1]

    country_name = _country_code_to_name(country_code) if country_code else None
    return (country_name, state, city)


def best_affiliation_record(
    aff_list: List[str],
    fips_by_city_state: Dict[Tuple[str, str], str],
    fips_by_city_only: Dict[str, str],
    cities_by_state: Dict[str, set],
) -> Tuple[str, str, str, str]:
    """
    Choose one affiliation-derived location per author.
    Preference:
      1) US with FIPS
      2) Non-US with country
      3) Any with some signal
      4) Otherwise blanks
    """
    candidates = []
    # Build a US city name set once from fips map (lowercased)
    us_city_names = set(fips_by_city_only.keys())
    for raw in aff_list:
        if not raw or not str(raw).strip():
            continue
        # Keep both original and ASCII-folded for robust matching
        raw_str = str(raw)
        aff = normalize_text(raw_str)

        country, state, city = extract_with_spacy_gazetteer(aff, us_city_names)

        # Normalize state to full name; normalize country capitalization
        if state:
            state = state_to_full(state) or state
        if country:
            # standardize US naming
            cl = country.lower().replace(".", "")
            if cl in ("united states", "usa", "us", "u s a", "u s"):
                country = "United States"

        fips = ""
        if country and country == "United States":
            fips = find_fips_for_city_state(city, state, fips_by_city_state, fips_by_city_only)

        candidates.append((
            str(city or ""),
            str(state or ""),
            str(country or ""),
            str(fips or "")
        ))

    if not candidates:
        return ("", "", "", "")

    # Rank candidates
    def score(cand: Tuple[str, str, str, str]) -> Tuple[int, int, int]:
        city, state, country, fips = cand
        # higher is better
        return (
            1 if fips else 0,                      # has FIPS
            1 if country and country != "" else 0, # has country
            1 if (city or state) else 0            # any local signal
        )

    candidates.sort(key=score, reverse=True)
    return candidates[0]


def load_affiliations_from_authors(authors_csv: str) -> List[Tuple[str, List[str]]]:
    """
    Returns list of (author_id, [affiliation strings])
    The 'affiliation' column is a JSON array as a string.
    """
    df = pd.read_csv(authors_csv, dtype=str, keep_default_na=False)
    if "author_id" not in df.columns or "affiliation" not in df.columns:
        raise RuntimeError("authors CSV must include 'author_id' and 'affiliation' columns")

    out: List[Tuple[str, List[str]]] = []
    for _, row in df.iterrows():
        author_id = str(row["author_id"]).strip()
        aff_raw = str(row["affiliation"]).strip()
        aff_list: List[str] = []
        if aff_raw:
            try:
                parsed = json.loads(aff_raw)
                if isinstance(parsed, list):
                    aff_list = [str(x) for x in parsed if x is not None]
                elif isinstance(parsed, str) and parsed:
                    aff_list = [parsed]
            except Exception:
                # if not valid JSON, treat as single string
                aff_list = [aff_raw]
        out.append((author_id, aff_list))
    return out


def write_affiliation_csv(records: List[Dict[str, str]], output_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["author_id", "Aff_city", "Aff_state", "Aff_country", "Aff_FIPS"]
        )
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate affiliation_node.csv from authors CSV.")
    parser.add_argument("--authors-csv", type=str, default=AUTHORS_CSV_DEFAULT, help="Path to authors CSV")
    parser.add_argument("--uscities-csv", type=str, default=USCITIES_DEFAULT, help="Path to uscities.csv")
    parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR_DEFAULT, help="Directory to write output CSV")
    args = parser.parse_args()

    fips_by_city_state, fips_by_city_only, cities_by_state = load_fips_map(args.uscities_csv)
    pairs = load_affiliations_from_authors(args.authors_csv)

    out_records: List[Dict[str, str]] = []
    for author_id, aff_list in pairs:
        city, state, country, fips = best_affiliation_record(
            aff_list, fips_by_city_state, fips_by_city_only, cities_by_state
        )
        out_records.append({
            "author_id": author_id,
            "Aff_city": city,
            "Aff_state": state,
            "Aff_country": country,
            "Aff_FIPS": fips,
        })

    output_path = os.path.join(args.output_dir, OUTPUT_FILENAME)
    write_affiliation_csv(out_records, output_path)
    print(f"Wrote {len(out_records)} rows -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
