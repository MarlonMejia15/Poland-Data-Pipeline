"""
transform_udsc_periodic_silver.py

Reads the UdSC first-half workbooks from Azure Raw and extracts the official
January-June residence totals from Arkusz11. Arkusz9 is intentionally not
used because it contains June-only activity.
"""

import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv


load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

RAW_CONTAINER = "raw"
CURATED_CONTAINER = "curated"
RAW_MANIFEST_PATH = "udsc/periodic/_manifest.json"
SILVER_DIRECTORY = "silver/udsc/residence/periodic"
LOCAL_OUTPUT_DIRECTORY = Path("data/silver/udsc/residence/periodic")
SOURCE_SHEET = "Arkusz11"

PERMIT_TYPE_MAP = {
    "pobyt czasowy": "temporary_residence",
    "pobyt stały": "permanent_residence",
    "pobyt rezyd. ue": "long_term_eu_residence",
}

METRIC_MAP = {
    "wnioski": "applications",
    "pozytywna": "positive",
    "negatywna": "negative",
    "umorzenie": "discontinued",
}

EXPECTED_VALUES = {
    2025: {
        "temporary_residence": {
            "applications": 347_679,
            "positive": 169_437,
            "negative": 13_529,
            "discontinued": 5_541,
        },
        "permanent_residence": {
            "applications": 14_104,
            "positive": 8_391,
            "negative": 1_378,
            "discontinued": 581,
        },
        "long_term_eu_residence": {
            "applications": 16_973,
            "positive": 9_711,
            "negative": 1_504,
            "discontinued": 1_029,
        },
    },
    2026: {
        "temporary_residence": {
            "applications": 415_823,
            "positive": 133_916,
            "negative": 12_440,
            "discontinued": 8_371,
        },
        "permanent_residence": {
            "applications": 13_876,
            "positive": 7_374,
            "negative": 1_272,
            "discontinued": 793,
        },
        "long_term_eu_residence": {
            "applications": 19_588,
            "positive": 9_939,
            "negative": 1_413,
            "discontinued": 1_046,
        },
    },
}


def normalize_text(value):
    """Normalizes Polish labels for controlled mappings."""

    return " ".join(str(value).strip().lower().split())


def validate_credentials():
    """Checks that the Azure Storage credentials exist."""

    if not STORAGE_ACCOUNT_NAME or not STORAGE_ACCOUNT_KEY:
        raise ValueError(
            "Missing AZURE_STORAGE_ACCOUNT_NAME or "
            "AZURE_STORAGE_ACCOUNT_KEY in .env"
        )


def get_file_system_clients():
    """Creates clients for the raw and curated containers."""

    validate_credentials()
    service_client = DataLakeServiceClient(
        account_url=(
            f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        ),
        credential=STORAGE_ACCOUNT_KEY,
    )
    return (
        service_client.get_file_system_client(RAW_CONTAINER),
        service_client.get_file_system_client(CURATED_CONTAINER),
    )


def download_bytes(file_system_client, path):
    """Downloads one Data Lake file as bytes."""

    return (
        file_system_client.get_file_client(path)
        .download_file()
        .readall()
    )


def ensure_directory(file_system_client, directory_path):
    """Creates every level of an Azure Data Lake directory."""

    current_path = ""
    for part in directory_path.split("/"):
        current_path = f"{current_path}/{part}" if current_path else part
        try:
            file_system_client.get_directory_client(
                current_path
            ).create_directory()
        except ResourceExistsError:
            pass


def upload_bytes(file_system_client, path, data):
    """Uploads bytes to Azure Curated."""

    parent_directory = str(Path(path).parent).replace("\\", "/")
    ensure_directory(file_system_client, parent_directory)
    file_system_client.get_file_client(path).upload_data(
        data,
        overwrite=True,
    )


def load_source_workbooks(raw_client):
    """Downloads both workbooks listed in the Raw manifest."""

    manifest = json.loads(
        download_bytes(raw_client, RAW_MANIFEST_PATH).decode("utf-8")
    )
    workbooks = {}

    for file_metadata in manifest["files"]:
        year = int(file_metadata["year"])
        azure_path = file_metadata["azure_path"]
        if azure_path.startswith(f"{RAW_CONTAINER}/"):
            azure_path = azure_path[len(RAW_CONTAINER) + 1 :]

        workbooks[year] = {
            "filename": file_metadata["original_filename"],
            "azure_path": azure_path,
            "period_start_date": file_metadata["period_start_date"],
            "period_end_date": file_metadata["period_end_date"],
            "half_number": int(file_metadata["half_number"]),
            "is_complete_year": bool(file_metadata["is_complete_year"]),
            "bytes": download_bytes(raw_client, azure_path),
        }

    if set(workbooks) != set(EXPECTED_VALUES):
        raise ValueError(f"Unexpected periodic years: {sorted(workbooks)}")
    return workbooks


def extract_first_half_totals(workbook, reference_year, processed_at_utc):
    """Extracts the cumulative H1 residence totals from Arkusz11."""

    source = pd.read_excel(
        io.BytesIO(workbook["bytes"]),
        sheet_name=SOURCE_SHEET,
        dtype=object,
        engine="openpyxl",
    )
    required_columns = {"Opis_rozstrzygniecia", "Liczba", "Opis"}
    if not required_columns.issubset(source.columns):
        raise ValueError(
            f"{workbook['filename']} has unexpected Arkusz11 columns"
        )

    application_records = []
    decision_records = []
    seen_pairs = set()

    for row in source.to_dict("records"):
        permit_label = normalize_text(row["Opis"])
        metric_label = normalize_text(row["Opis_rozstrzygniecia"])
        if permit_label not in PERMIT_TYPE_MAP:
            raise ValueError(f"Unknown residence label: {row['Opis']}")
        if metric_label not in METRIC_MAP:
            raise ValueError(
                f"Unknown outcome label: {row['Opis_rozstrzygniecia']}"
            )

        permit_type = PERMIT_TYPE_MAP[permit_label]
        metric = METRIC_MAP[metric_label]
        count = int(row["Liczba"])
        pair = (permit_type, metric)
        if pair in seen_pairs:
            raise ValueError(f"Duplicate periodic metric: {pair}")
        seen_pairs.add(pair)

        common = {
            "source_system": "UdSC",
            "reference_year": reference_year,
            "period_key": reference_year * 10 + workbook["half_number"],
            "period_start_date": workbook["period_start_date"],
            "period_end_date": workbook["period_end_date"],
            "period_type": "half_year",
            "half_number": workbook["half_number"],
            "is_complete_year": workbook["is_complete_year"],
            "permit_type": permit_type,
            "source_file": workbook["filename"],
            "source_sheet": SOURCE_SHEET,
            "pipeline_processed_at_utc": processed_at_utc,
        }

        if metric == "applications":
            application_records.append(
                {**common, "application_count": count}
            )
        else:
            decision_records.append(
                {
                    **common,
                    "decision_outcome": metric,
                    "decision_count": count,
                }
            )

    expected_pairs = {
        (permit_type, metric)
        for permit_type, values in EXPECTED_VALUES[reference_year].items()
        for metric in values
    }
    if seen_pairs != expected_pairs:
        raise ValueError(
            f"Missing or additional H1 metrics for {reference_year}"
        )
    return application_records, decision_records


def validate_official_values(applications, decisions):
    """Reconciles every extracted value with the published workbooks."""

    application_lookup = {
        (int(row.reference_year), row.permit_type): int(row.application_count)
        for row in applications.itertuples()
    }
    decision_lookup = {
        (
            int(row.reference_year),
            row.permit_type,
            row.decision_outcome,
        ): int(row.decision_count)
        for row in decisions.itertuples()
    }

    for year, permit_values in EXPECTED_VALUES.items():
        for permit_type, metrics in permit_values.items():
            if application_lookup[(year, permit_type)] != metrics["applications"]:
                raise ValueError(f"Application validation failed: {year} {permit_type}")
            for outcome in ("positive", "negative", "discontinued"):
                if decision_lookup[(year, permit_type, outcome)] != metrics[outcome]:
                    raise ValueError(
                        f"Decision validation failed: {year} "
                        f"{permit_type} {outcome}"
                    )


def save_and_upload_dataframe(dataframe, base_filename, curated_client):
    """Writes a periodic Silver table locally and to Azure."""

    LOCAL_OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)
    csv_path = LOCAL_OUTPUT_DIRECTORY / f"{base_filename}.csv"
    parquet_path = LOCAL_OUTPUT_DIRECTORY / f"{base_filename}.parquet"
    dataframe.to_csv(csv_path, index=False, encoding="utf-8-sig")
    dataframe.to_parquet(parquet_path, index=False, engine="pyarrow")

    for local_path in (csv_path, parquet_path):
        azure_path = f"{SILVER_DIRECTORY}/{local_path.name}"
        upload_bytes(curated_client, azure_path, local_path.read_bytes())
        print(f"Uploaded to curated/{azure_path}")


def upload_manifest(curated_client, applications, decisions, processed_at_utc):
    """Uploads Silver lineage, grain, and validation metadata."""

    manifest = {
        "source_system": "UdSC",
        "source_raw_manifest": f"{RAW_CONTAINER}/{RAW_MANIFEST_PATH}",
        "layer": "silver",
        "period_type": "half_year",
        "years": sorted(EXPECTED_VALUES),
        "is_complete_year": False,
        "processed_at_utc": processed_at_utc,
        "source_sheet_used": SOURCE_SHEET,
        "source_sheet_excluded": "Arkusz9 (June-only activity)",
        "grain": {
            "applications": "period and permit type; national total",
            "decisions": "period, permit type, and outcome; national total",
        },
        "row_counts": {
            "applications": len(applications),
            "decisions": len(decisions),
        },
        "validation": {
            "official_values_reconciled": True,
            "h1_2025_and_h1_2026_only": True,
            "citizenship_breakdown_available": False,
        },
    }
    upload_bytes(
        curated_client,
        f"{SILVER_DIRECTORY}/_manifest.json",
        json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
    )


def main():
    """Runs the complete UdSC periodic Raw-to-Silver transformation."""

    raw_client, curated_client = get_file_system_clients()
    workbooks = load_source_workbooks(raw_client)
    processed_at_utc = datetime.now(timezone.utc).isoformat()
    application_records = []
    decision_records = []

    for year in sorted(workbooks):
        print(f"Processing UdSC H1 report {year}...")
        applications, decisions = extract_first_half_totals(
            workbooks[year],
            year,
            processed_at_utc,
        )
        application_records.extend(applications)
        decision_records.extend(decisions)

    applications = pd.DataFrame(application_records).sort_values(
        ["reference_year", "permit_type"]
    ).reset_index(drop=True)
    decisions = pd.DataFrame(decision_records).sort_values(
        ["reference_year", "permit_type", "decision_outcome"]
    ).reset_index(drop=True)
    validate_official_values(applications, decisions)

    print("Silver periodic validation passed:")
    print(f"  Applications: {len(applications):,} rows")
    print(f"  Decisions: {len(decisions):,} rows")

    save_and_upload_dataframe(
        applications,
        "udsc_residence_applications_h1_2025_2026",
        curated_client,
    )
    save_and_upload_dataframe(
        decisions,
        "udsc_residence_decisions_h1_2025_2026",
        curated_client,
    )
    upload_manifest(
        curated_client,
        applications,
        decisions,
        processed_at_utc,
    )
    print("Done. UdSC periodic Raw-to-Silver transformation completed.")


if __name__ == "__main__":
    main()
