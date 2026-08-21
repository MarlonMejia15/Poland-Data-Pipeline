"""
ingest_gus_honduras_values.py

Downloads a small numerical test extract from GUS BDL for Census 2021:
people with Honduran citizenship and people born in Honduras. Results are
limited to Poland (level 0) and voivodships (level 2).
"""

import time
from datetime import datetime, timezone

from ingest_gus_data import (
    REQUEST_DELAY_SECONDS,
    get_all_pages,
    save_raw_locally,
    upload_to_datalake,
)


DATA_YEAR = 2021

VARIABLES = {
    1662527: "Usual residents with Honduran citizenship - total",
    1663114: "Usual residents born in Honduras - total",
}

UNIT_LEVELS = {
    0: "Poland",
    2: "Voivodship",
}


def get_honduras_values():
    """Fetches the selected Honduras variables at national and regional level."""
    all_results = []
    query_summaries = []
    total_queries = len(VARIABLES) * len(UNIT_LEVELS)
    query_number = 0

    for variable_id, variable_name in VARIABLES.items():
        for unit_level, unit_level_name in UNIT_LEVELS.items():
            query_number += 1
            print(
                f"Fetching variable {variable_id} at level {unit_level} "
                f"({query_number}/{total_queries})..."
            )

            data_page = get_all_pages(
                endpoint=f"data/by-variable/{variable_id}",
                params={
                    "format": "json",
                    "lang": "en",
                    "year": DATA_YEAR,
                    "unit-Level": unit_level,
                },
            )

            for row in data_page["results"]:
                row["variableId"] = variable_id
                row["variableName"] = variable_name
                row["requestedUnitLevel"] = unit_level
                row["requestedUnitLevelName"] = unit_level_name
                all_results.append(row)

            query_summaries.append(
                {
                    "variableId": variable_id,
                    "variableName": variable_name,
                    "unitLevel": unit_level,
                    "unitLevelName": unit_level_name,
                    "recordCount": data_page["totalRecords"],
                }
            )

            if query_number < total_queries:
                time.sleep(REQUEST_DELAY_SECONDS)

    return {
        "source": "https://bdl.stat.gov.pl/api/v1/data/by-variable",
        "extractedAtUtc": datetime.now(timezone.utc).isoformat(),
        "year": DATA_YEAR,
        "queries": query_summaries,
        "totalRecords": len(all_results),
        "results": all_results,
    }


if __name__ == "__main__":
    print("Fetching Honduras Census 2021 numerical values...")
    honduras_values = get_honduras_values()

    for query in honduras_values["queries"]:
        print(
            f"Variable {query['variableId']} - {query['unitLevelName']}: "
            f"{query['recordCount']} records"
        )
    print(f"Total records downloaded: {honduras_values['totalRecords']}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"gus_census_2021_honduras_values_{timestamp}.json"

    local_path = save_raw_locally(honduras_values, filename)
    upload_to_datalake(local_path)

    print("Done. Numerical values were downloaded and uploaded to Azure.")