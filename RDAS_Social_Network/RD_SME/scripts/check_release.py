#!/usr/bin/env python3
"""Check staged public files for obvious private paths, credentials, and data."""

from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[1]
RULES = {
    "account path or host": re.compile(
        r"/home/|/ncats/|/vast/|\b[\w.-]+\.nih\.gov\b", re.I
    ),
    "credential literal": re.compile(
        r"\b(?:password|passwd|token|secret|api[_-]?key)\s*=\s*"
        r"[\"'][^\"']+[\"']",
        re.I,
    ),
    "private key": re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----"),
}
EXCLUDED_PATHS = (
    "data/processed/",
    "data/external/",
    "graph_for_paper/graphs/",
    "step2_experiment_clustering/outputs/",
)
EXCLUDED_NAMES = {
    "sample_cluster_assignments.csv",
    "sampled_embeddings_n1000_seed42.csv",
}


def main() -> int:
    staged = list(filter(None, subprocess.check_output(
        ["git", "ls-files", "-z"], cwd=ROOT
    ).decode().split("\0")))
    findings = []
    for name in staged:
        if name.startswith(EXCLUDED_PATHS) or Path(name).name in EXCLUDED_NAMES:
            findings.append(f"{name}: excluded data path")
            continue
        path = ROOT / name
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            findings.append(f"{name}: binary file needs review")
            continue
        if name == "scripts/check_release.py":
            continue  # Its rule definitions contain the patterns it searches for.
        for number, line in enumerate(lines, 1):
            for label, rule in RULES.items():
                if rule.search(line):
                    findings.append(f"{name}:{number}: {label}")
    if findings:
        print("Review these staged files before publication:")
        print("\n".join(findings))
        return 1
    print(f"Checked {len(staged)} staged files; no obvious private values found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
