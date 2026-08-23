"""
ingest_gus_country_variables.py

Downloads variable metadata only for the selected Census 2021 subgroups that
describe citizenship, country of birth, and temporary immigrants' previous
country groups. No numerical statistical values are downloaded in this step.
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

SELECTED_SUBGROUPS = {
    "P4306": "Usual residence population by citizenship and sex",
    "P4307": "Usual residence population by countries of birth and sex",
    "P4336": (
        "Immigrants temporarily staying in Poland by groups of countries "
        "of previous residence, economic age groups and sex"
    ),
}


def get_selected_country_variables():
    """Fetches and combines variable metadata for selected census subgroups."""
    all_variables = []
    subgroup_summaries = []

    for index, (subgroup_id, subgroup_name) in enumerate(
        SELECTED_SUBGROUPS.items(),
        start=1,
    ):
        print(
            f"Fetching variables for {subgroup_id} "
            f"({index}/{len(SELECTED_SUBGROUPS)})..."
        )

        variable_page = get_all_pages(
            endpoint="variables",
            params={
                "format": "json",
                "lang": "en",
                "sort": "id",
                "subject-Id": subgroup_id,
                "year": DATA_YEAR,
            },
        )

        for variable in variable_page["results"]:
            variable["subgroupId"] = subgroup_id
            variable["subgroupName"] = subgroup_name
            all_variables.append(variable)

        subgroup_summaries.append(
            {
                "subgroupId": subgroup_id,
                "subgroupName": subgroup_name,
                "variableCount": variable_page["totalRecords"],
            }
        )

        if index < len(SELECTED_SUBGROUPS):
            time.sleep(REQUEST_DELAY_SECONDS)

    return {
        "source": "https://bdl.stat.gov.pl/api/v1/variables",
        "extractedAtUtc": datetime.now(timezone.utc).isoformat(),
        "year": DATA_YEAR,
        "subgroups": subgroup_summaries,
        "totalRecords": len(all_variables),
        "results": all_variables,
    }


if __name__ == "__main__":
    print("Fetching selected country-related Census 2021 variables...")
    country_variables = get_selected_country_variables()

    for subgroup in country_variables["subgroups"]:
        print(
            f"{subgroup['subgroupId']}: "
            f"{subgroup['variableCount']} variables"
        )
    print(f"Total variables downloaded: {country_variables['totalRecords']}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"gus_census_2021_country_variables_{timestamp}.json"

    local_path = save_raw_locally(
        country_variables,
        filename,
        local_directory="data/raw/gus/discovery",
    )
    upload_to_datalake(
        local_path,
        remote_directory="gus/discovery",
    )

    print("Done. No numerical statistical values were downloaded in this step.")
