"""
ingest_gus_data.py

Downloads the complete top-level subject catalog and its direct child groups
from the GUS BDL API, then stores the combined JSON results locally and in the
Azure Data Lake "raw" container.

GUS BDL API docs: https://api.stat.gov.pl/Home/BdlApi
"""

import json
import os
import time
from datetime import datetime, timezone

import requests
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Load credentials from .env
load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

BDL_BASE_URL = "https://bdl.stat.gov.pl/api/v1"
PAGE_SIZE = 100
REQUEST_TIMEOUT = 30
REQUEST_DELAY_SECONDS = 0.3

# This applies to statistical values downloaded in the next stage.
# Subject/category metadata itself does not have a year field.
DATA_START_YEAR = 2020


def create_http_session():
    """Creates an HTTP session with retries for temporary API failures."""
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    return session


def get_all_pages(endpoint, params=None, session=None):
    """Fetches and combines every page returned by a GUS BDL endpoint."""
    url = f"{BDL_BASE_URL}/{endpoint.lstrip('/')}"
    base_params = dict(params or {})
    page = 0
    all_results = []
    total_records = None
    owns_session = session is None
    http_session = session or create_http_session()

    try:
        while True:
            page_params = {
                **base_params,
                "page": page,
                "page-size": PAGE_SIZE,
            }

            print(f"Fetching page {page}...")
            response = http_session.get(
                url,
                params=page_params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()

            page_results = payload.get("results", [])
            if not isinstance(page_results, list):
                raise ValueError("Unexpected GUS response: 'results' is not a list.")

            all_results.extend(page_results)

            if total_records is None:
                total_records = payload.get("totalRecords")

            if total_records is not None:
                # Continue even if the API returns fewer rows than PAGE_SIZE.
                # Some endpoints may enforce their own smaller page size.
                if len(all_results) >= total_records:
                    break
            else:
                # Fallback for endpoints that do not supply totalRecords.
                if not page_results or len(page_results) < PAGE_SIZE:
                    break

            if not page_results:
                raise RuntimeError(
                    "GUS returned an empty page before totalRecords was reached."
                )

            page += 1
    finally:
        if owns_session:
            http_session.close()

    return {
        "source": url,
        "extractedAtUtc": datetime.now(timezone.utc).isoformat(),
        "parameters": base_params,
        "totalRecords": len(all_results),
        "results": all_results,
    }


def get_subjects():
    """Fetches all top-level statistical subjects/categories from GUS BDL."""
    return get_all_pages(
        endpoint="subjects",
        params={
            "format": "json",
            "lang": "en",
            "sort": "id",
        },
    )


def get_subject_groups(subjects):
    """Fetches the direct child groups of every top-level subject."""
    subject_rows = subjects.get("results", [])
    all_groups = []

    with create_http_session() as session:
        for index, subject in enumerate(subject_rows, start=1):
            subject_id = subject["id"]
            subject_name = subject.get("name")
            print(
                f"Fetching groups for {subject_id} "
                f"({index}/{len(subject_rows)})..."
            )

            group_page = get_all_pages(
                endpoint="subjects",
                params={
                    "format": "json",
                    "lang": "en",
                    "sort": "id",
                    "parent-Id": subject_id,
                },
                session=session,
            )

            for group in group_page["results"]:
                group["parentId"] = subject_id
                group["parentName"] = subject_name
                all_groups.append(group)

            # Stay below the anonymous limit of five requests per second.
            if index < len(subject_rows):
                time.sleep(REQUEST_DELAY_SECONDS)

    return {
        "source": f"{BDL_BASE_URL}/subjects",
        "extractedAtUtc": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "format": "json",
            "lang": "en",
            "sort": "id",
            "relationship": "direct children of top-level subjects",
        },
        "parentSubjectCount": len(subject_rows),
        "totalRecords": len(all_groups),
        "results": all_groups,
    }


def save_raw_locally(data, filename):
    """Saves the combined JSON response to a local data folder."""
    os.makedirs("data", exist_ok=True)
    filepath = os.path.join("data", filename)

    with open(filepath, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

    print(f"Saved locally: {filepath}")
    return filepath


def validate_azure_config():
    """Stops early with a clear message if Azure credentials are missing."""
    missing = []

    if not STORAGE_ACCOUNT_NAME:
        missing.append("AZURE_STORAGE_ACCOUNT_NAME")
    if not STORAGE_ACCOUNT_KEY:
        missing.append("AZURE_STORAGE_ACCOUNT_KEY")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


def upload_to_datalake(local_filepath, container="raw"):
    """Uploads a local file to an Azure Data Lake Gen2 container."""
    validate_azure_config()

    account_url = (
        f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    )
    service_client = DataLakeServiceClient(
        account_url=account_url,
        credential=STORAGE_ACCOUNT_KEY,
    )
    file_system_client = service_client.get_file_system_client(
        file_system=container
    )

    file_name = os.path.basename(local_filepath)
    file_client = file_system_client.get_file_client(file_name)

    with open(local_filepath, "rb") as file:
        file_client.upload_data(file.read(), overwrite=True)

    print(f"Uploaded to Data Lake container '{container}': {file_name}")


if __name__ == "__main__":
    print("Fetching all top-level subjects from GUS BDL API...")
    subjects = get_subjects()
    print(f"Subjects downloaded: {subjects['totalRecords']}")

    print("Fetching direct child groups for all subjects...")
    subject_groups = get_subject_groups(subjects)
    print(f"Subject groups downloaded: {subject_groups['totalRecords']}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    subjects_filename = f"gus_subjects_{timestamp}.json"
    groups_filename = f"gus_subject_groups_{timestamp}.json"

    subjects_local_path = save_raw_locally(subjects, subjects_filename)
    groups_local_path = save_raw_locally(subject_groups, groups_filename)

    upload_to_datalake(subjects_local_path)
    upload_to_datalake(groups_local_path)

    print(f"Done. Statistical data will be limited to {DATA_START_YEAR}+.")
