import os
import pandas as pd
import numpy as np

INPUT_CSV = "data/processed/csv_files/affiliation_node.csv"
OUTPUT_DIR = "data/processed/figures"


US_ALIASES = {
    "us",
    "usa",
    "u.s.",
    "u.s.a.",
    "united states",
    "united states of america",
}


def normalize_country(val: object) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    text = str(val).strip()
    return text


def is_us_country(val: str) -> bool:
    v = val.strip().lower()
    return v in US_ALIASES


def summarize() -> pd.DataFrame:
    df = pd.read_csv(INPUT_CSV, dtype=str)
    # Coerce columns and clean
    for col in ["Aff_city", "Aff_state", "Aff_country", "Aff_FIPS"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    total = len(df)

    # U.S. county resolution via valid 5-digit FIPS
    is_valid_fips = df["Aff_FIPS"].str.fullmatch(r"\d{5}", na=False)
    us_county_resolution = int(is_valid_fips.sum())

    # International resolution (has country AND not US)
    country_norm = df["Aff_country"].apply(normalize_country)
    is_international = country_norm.astype(bool) & ~country_norm.str.lower().isin(US_ALIASES)
    international_resolution = int(is_international.sum())

    # Resolved location nodes = US county OR International
    resolved_location_nodes = int((is_valid_fips | is_international).sum())

    # Unassigned = not resolved
    unassigned = int(total - resolved_location_nodes)

    rows = [
        {
            "Metric": "Total Researcher Affiliations [Total N]",
            "Count": total,
            "Percentage": 100.0 if total > 0 else 0.0,
            "Note": "Total affiliation rows in file.",
        },
        {
            "Metric": "Resolved Location Nodes",
            "Count": resolved_location_nodes,
            "Percentage": (resolved_location_nodes / total * 100.0) if total > 0 else 0.0,
            "Note": "Standardized nodes in Knowledge Graph (US + International).",
        },
        {
            "Metric": "U.S. County Resolution",
            "Count": us_county_resolution,
            "Percentage": (us_county_resolution / total * 100.0) if total > 0 else 0.0,
            "Note": "Linked to FIPS codes via Reference U.S. Cities.",
        },
        {
            "Metric": "International Resolution",
            "Count": international_resolution,
            "Percentage": (international_resolution / total * 100.0) if total > 0 else 0.0,
            "Note": "Validated via country-specific gazetteers.",
        },
        {
            "Metric": "Unassigned Records",
            "Count": unassigned,
            "Percentage": (unassigned / total * 100.0) if total > 0 else 0.0,
            "Note": "Omitted due to insufficient/ambiguous signal.",
        },
    ]
    out = pd.DataFrame(rows)
    # Round percentages to one decimal
    out["Percentage"] = out["Percentage"].map(lambda x: f"{x:.1f}%")
    return out


def save_outputs(table: pd.DataFrame) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, "affiliation_summary_table.csv")
    md_path = os.path.join(OUTPUT_DIR, "affiliation_summary_table.md")
    table.to_csv(csv_path, index=False)
    # Markdown
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("| Metric | Count | Percentage | Note |\n")
        f.write("|---|---:|---:|---|\n")
        for _, r in table.iterrows():
            f.write(f"| {r['Metric']} | {r['Count']} | {r['Percentage']} | {r['Note']} |\n")


def main() -> None:
    table = summarize()
    save_outputs(table)
    print(table.to_string(index=False))


if __name__ == "__main__":
    main()
