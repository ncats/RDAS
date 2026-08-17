import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv


NCTID = "NCT06487273"
_REPO_DIR = Path(__file__).resolve().parents[1]
_Z_ALERT_DIR = _REPO_DIR / "Z_Alert"

sys.path.extend([
    str(_REPO_DIR),
    str(_Z_ALERT_DIR),
])

load_dotenv(_REPO_DIR / ".env")

from baseclass.conn import DBConnection
from pipelines.pipeline_2_clinical_trial.clinical_trial_study_change_handler import ClinicalTrialStudyChangeHandler


class ConsoleLogger:

    def info(self, message: str) -> None:

        print(message)


def main() -> None:

    '''
    Compare one live ClinicalTrials.gov study response with the stored canonical
    JSON in rdas_db.clinical_trial_unique.

    Run this file directly when checking the change handler:
        python test/clinical_trial_study_change_handler_test.py
    '''
    studies_api = (
        os.getenv("CLINICAL_TRIAL_STUDIES_API")
        or os.getenv("CLINICAL_TRAIL_STUDY_URL")
        or "https://clinicaltrials.gov/api/v2/studies"
    ).rstrip("/")

    response = requests.get(f"{studies_api}/{NCTID}", timeout=30)
    response.raise_for_status()
    fetched_study = response.json()

    mysql = DBConnection().mysql_conn()

    if mysql is None:
        raise RuntimeError("Unable to create a MySQL connection. Check MYSQL_* values in .env.")

    handler = ClinicalTrialStudyChangeHandler(mysql, ConsoleLogger())

    try:
        stored_study = handler.load_unique_study(NCTID)

        if stored_study is None:
            print(f"No clinical_trial_unique.studies row found for NCTID={NCTID}")
            print("is_same=False")
            print("differences=")
            print(json.dumps([
                {
                    "path": "$",
                    "reason": "missing_from_clinical_trial_unique",
                    "stored": None,
                    "fetched": "Fetched study JSON exists in ClinicalTrials.gov.",
                }
            ], indent=2))
            return

        is_same, differences = handler.compare_studies(stored_study, fetched_study)

        print(f"NCTID={NCTID}")
        print(f"is_same={is_same}")
        print("differences=")
        print(json.dumps(differences, ensure_ascii=False, indent=2, default=str))

    finally:
        if mysql.is_connected():
            mysql.close()


if __name__ == "__main__":

    main()
