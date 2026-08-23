"""
ingest_gus_all_citizenships.py

Downloads Census 2021 usual-residence counts for every foreign citizenship,
plus stateless/unknown categories, for Poland and all 16 voivodships.

The script reads the most recent country-variable metadata JSON from the local
GUS Raw layer and queries GUS BDL by territorial unit in batches to stay below
anonymous limits.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from ingest_gus_data import (
    REQUEST_DELAY_SECONDS,
    get_all_pages,
    save_raw_locally,
    upload_to_datalake,
)

DATA_YEAR = 2021
VARIABLE_BATCH_SIZE = 40
METADATA_PATTERN = "gus_census_2021_country_variables_*.json"

UNITS = {
    "000000000000": {"name": "POLAND", "level": 0},
    "011200000000": {"name": "MAŁOPOLSKIE", "level": 2},
    "012400000000": {"name": "ŚLĄSKIE", "level": 2},
    "020800000000": {"name": "LUBUSKIE", "level": 2},
    "023000000000": {"name": "WIELKOPOLSKIE", "level": 2},
    "023200000000": {"name": "ZACHODNIOPOMORSKIE", "level": 2},
    "030200000000": {"name": "DOLNOŚLĄSKIE", "level": 2},
    "031600000000": {"name": "OPOLSKIE", "level": 2},
    "040400000000": {"name": "KUJAWSKO-POMORSKIE", "level": 2},
    "042200000000": {"name": "POMORSKIE", "level": 2},
    "042800000000": {"name": "WARMIŃSKO-MAZURSKIE", "level": 2},
    "051000000000": {"name": "ŁÓDZKIE", "level": 2},
    "052600000000": {"name": "ŚWIĘTOKRZYSKIE", "level": 2},
    "060600000000": {"name": "LUBELSKIE", "level": 2},
    "061800000000": {"name": "PODKARPACKIE", "level": 2},
    "062000000000": {"name": "PODLASKIE", "level": 2},
    "071400000000": {"name": "MAZOWIECKIE", "level": 2},
}

EXCLUDED_CITIZENSHIP_LABELS = {
    "total",
    "polish citizenship",
}


def find_latest_metadata_file():
    """Returns the newest country-variable metadata file from local Raw."""
    metadata_root = Path("data/raw/gus")
    files = list(metadata_root.rglob(METADATA_PATTERN))

    if not files:
        raise FileNotFoundError(
            f"No country-variable metadata matching "
            f"'{METADATA_PATTERN}' was found inside "
            f"'{metadata_root}'. Run ingest_gus_country_variables.py first."
        )

    return max(
        files,
        key=lambda path: path.stat().st_mtime,
    )


def classify_citizenship(label):
    """Classifies country rows separately from aggregate/special rows."""
    normalized = label.strip().lower()
    if normalized == "non-polish citizenship":
        return "aggregate"
    if normalized == "stateless":
        return "stateless"
    if normalized == "unknown":
        return "unknown"
    return "country"


def load_citizenship_variables(metadata_path):
    """Loads total-sex P4306 variables and excludes Polish/overall totals."""
    with open(metadata_path, "r", encoding="utf-8") as file:
        metadata = json.load(file)

    selected = []
    for variable in metadata.get("results", []):
        citizenship_label = str(variable.get("n2", "")).strip()
        if variable.get("subgroupId") != "P4306":
            continue
        if variable.get("n1") != "total":
            continue
        if citizenship_label.lower() in EXCLUDED_CITIZENSHIP_LABELS:
            continue

        selected.append(
            {
                "id": int(variable["id"]),
                "citizenship": citizenship_label,
                "citizenshipType": classify_citizenship(citizenship_label),
            }
        )

    if not selected:
        raise ValueError("No P4306 total citizenship variables were found.")
    return selected


def chunked(items, size):
    """Yields fixed-size batches from a list."""
    for start in range(0, len(items), size):
        yield items[start : start + size]


def get_all_citizenship_values(variable_metadata):
    """Downloads all selected variables for Poland and each voivodship."""
    metadata_by_id = {item["id"]: item for item in variable_metadata}
    variable_batches = list(chunked(variable_metadata, VARIABLE_BATCH_SIZE))
    total_queries = len(UNITS) * len(variable_batches)
    query_number = 0
    all_results = []
    query_summaries = []

    for unit_id, unit in UNITS.items():
        for batch_number, batch in enumerate(variable_batches, start=1):
            query_number += 1
            variable_ids = [item["id"] for item in batch]
            print(
                f"Fetching {unit['name']} batch {batch_number}/"
                f"{len(variable_batches)} ({query_number}/{total_queries})..."
            )

            data_page = get_all_pages(
                endpoint=f"data/by-unit/{unit_id}",
                params={
                    "format": "json",
                    "lang": "en",
                    "year": DATA_YEAR,
                    "var-Id": variable_ids,
                },
            )

            for row in data_page["results"]:
                variable_id = int(row["id"])
                variable = metadata_by_id[variable_id]
                row["variableId"] = variable_id
                row["citizenship"] = variable["citizenship"]
                row["citizenshipType"] = variable["citizenshipType"]
                row["unitId"] = unit_id
                row["unitName"] = unit["name"]
                row["unitLevel"] = unit["level"]
                all_results.append(row)

            query_summaries.append(
                {
                    "unitId": unit_id,
                    "unitName": unit["name"],
                    "batchNumber": batch_number,
                    "requestedVariables": len(variable_ids),
                    "returnedRecords": data_page["totalRecords"],
                }
            )

            if query_number < total_queries:
                time.sleep(REQUEST_DELAY_SECONDS)

    return {
        "source": "https://bdl.stat.gov.pl/api/v1/data/by-unit",
        "extractedAtUtc": datetime.now(timezone.utc).isoformat(),
        "year": DATA_YEAR,
        "territoryCount": len(UNITS),
        "variableCount": len(variable_metadata),
        "countryCount": sum(
            item["citizenshipType"] == "country" for item in variable_metadata
        ),
        "queryCount": total_queries,
        "queries": query_summaries,
        "totalRecords": len(all_results),
        "results": all_results,
    }


if __name__ == "__main__":
    metadata_file = find_latest_metadata_file()
    print(f"Using metadata file: {metadata_file}")

    citizenship_variables = load_citizenship_variables(metadata_file)
    country_count = sum(
        item["citizenshipType"] == "country" for item in citizenship_variables
    )
    print(
        f"Selected {len(citizenship_variables)} variables, "
        f"including {country_count} countries."
    )

    citizenship_values = get_all_citizenship_values(citizenship_variables)
    print(f"Total records downloaded: {citizenship_values['totalRecords']}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"gus_census_2021_all_citizenships_{timestamp}.json"
    local_path = save_raw_locally(
        citizenship_values,
        filename,
        local_directory="data/raw/gus/census_2021/citizenship",
    )

    upload_to_datalake(
        local_path,
        remote_directory="gus/census_2021/citizenship",
    )

    print("Done. All citizenship values were uploaded to Azure.")
