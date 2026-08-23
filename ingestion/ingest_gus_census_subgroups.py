"""
ingest_gus_census_subgroups.py

Downloads only the subgroups under G640 (Census 2021 - Population) from the
GUS BDL API. This focused metadata extract is used to locate the exact IDs for
citizenship, country of birth, and temporary immigration indicators.
"""

from datetime import datetime

from ingest_gus_data import (
    get_all_pages,
    save_raw_locally,
    upload_to_datalake,
)


TARGET_CATEGORY_ID = "K31"
TARGET_CATEGORY_NAME = "NATIONAL CENSUSES"
TARGET_GROUP_ID = "G640"
TARGET_GROUP_NAME = "CENSUS 2021 - POPULATION"


def get_census_population_subgroups():
    """Fetches every direct child subgroup under G640."""
    subgroups = get_all_pages(
        endpoint="subjects",
        params={
            "format": "json",
            "lang": "en",
            "sort": "id",
            "parent-Id": TARGET_GROUP_ID,
        },
    )

    for subgroup in subgroups["results"]:
        subgroup["categoryId"] = TARGET_CATEGORY_ID
        subgroup["categoryName"] = TARGET_CATEGORY_NAME
        subgroup["parentId"] = TARGET_GROUP_ID
        subgroup["parentName"] = TARGET_GROUP_NAME

    subgroups["target"] = {
        "categoryId": TARGET_CATEGORY_ID,
        "categoryName": TARGET_CATEGORY_NAME,
        "groupId": TARGET_GROUP_ID,
        "groupName": TARGET_GROUP_NAME,
    }
    return subgroups


if __name__ == "__main__":
    print(
        "Fetching subgroups for "
        f"{TARGET_GROUP_ID} - {TARGET_GROUP_NAME}..."
    )
    census_subgroups = get_census_population_subgroups()
    print(f"Census subgroups downloaded: {census_subgroups['totalRecords']}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"gus_census_2021_subgroups_{timestamp}.json"

    local_path = save_raw_locally(
        census_subgroups,
        filename,
        local_directory="data/raw/gus/discovery",
    )
    upload_to_datalake(
        local_path,
        remote_directory="gus/discovery",
    )

    print("Done. No statistical values were downloaded in this step.")
