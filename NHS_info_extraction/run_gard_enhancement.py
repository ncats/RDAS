import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from extraction_core import TerminologyEnhancer

MODELS = {
    "gemma": "/ncats/users/liky/results/gemma/analyzed_studies_goldstandard_labeled_WITH_ID.json",
    "llama3": "/ncats/users/liky/results/llama3/analyzed_studies_goldstandard_labeled_WITH_ID.json",
    "athena": "/ncats/users/liky/results/athena/analyzed_studies_goldstandard_labeled_WITH_ID.json",
}


def run_model(model_name, in_path):
    out_path = in_path.replace("_WITH_ID.json", "_WITH_GARD.json")
    print(f"=== {model_name}: {in_path} ===", flush=True)

    with open(in_path) as f:
        data = json.load(f)

    enhancer = TerminologyEnhancer(enable_api_calls=True, verbose=False)

    total = len(data)
    for i, rec in enumerate(data):
        ec = rec.get("extracted_characteristics")
        if isinstance(ec, dict) and ec.get("disease_name"):
            ec["disease_name"] = enhancer.enhance_disease_name(ec["disease_name"])
        if (i + 1) % 200 == 0:
            print(f"  {model_name}: {i + 1}/{total} processed", flush=True)

    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    gard_total = enhancer.stats["gard_matched"] + enhancer.stats["gard_not_found"]
    pct = 100 * enhancer.stats["gard_matched"] / gard_total if gard_total else 0
    print(f"  {model_name} DONE: GARD {enhancer.stats['gard_matched']}/{gard_total} matched ({pct:.1f}%)", flush=True)
    print(f"  saved to {out_path}", flush=True)
    return model_name, enhancer.stats


if __name__ == "__main__":
    results = {}
    for name, path in MODELS.items():
        _, stats = run_model(name, path)
        results[name] = stats

    print("\n=== SUMMARY ===")
    for name, stats in results.items():
        total = stats["gard_matched"] + stats["gard_not_found"]
        pct = 100 * stats["gard_matched"] / total if total else 0
        print(f"{name}: GARD {stats['gard_matched']}/{total} matched ({pct:.1f}%)")
