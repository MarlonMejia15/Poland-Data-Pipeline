"""
transform_gus_citizenships.py

Reads the latest GUS Census 2021 citizenship JSON from the Azure Data Lake
"raw" container, flattens and validates it, and writes curated CSV and Parquet
files to:

    curated/silver/gus/census_2021/citizenship/

The current GUS extraction already contains English citizenship labels. Polish
voivodship names are standardized through a controlled reference mapping; this
is safer for geographic joins than machine-translating official place names.
Azure Translator will be used for unique Polish labels from future sources.
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
RAW_DIRECTORY = "gus/census_2021/citizenship"
CURATED_CONTAINER = "curated"
RAW_FILE_PREFIX = "gus_census_2021_all_citizenships_"
SILVER_DIRECTORY = "silver/gus/census_2021/citizenship"

CSV_FILENAME = "gus_citizenships_2021.csv"
PARQUET_FILENAME = "gus_citizenships_2021.parquet"

EXPECTED_YEAR = 2021
EXPECTED_TERRITORIES = 17
EXPECTED_VARIABLES = 190
EXPECTED_COUNTRIES = 187

# Controlled reference data is preferable to machine translation for official
# administrative geography names and future map joins.

TERRITORY_NAMES_EN = {
    "POLAND": "Poland",
    "MAŁOPOLSKIE": "Lesser Poland",
    "ŚLĄSKIE": "Silesian",
    "LUBUSKIE": "Lubusz",
    "WIELKOPOLSKIE": "Greater Poland",
    "ZACHODNIOPOMORSKIE": "West Pomeranian",
    "DOLNOŚLĄSKIE": "Lower Silesian",
    "OPOLSKIE": "Opole",
    "KUJAWSKO-POMORSKIE": "Kuyavian-Pomeranian",
    "POMORSKIE": "Pomeranian",
    "WARMIŃSKO-MAZURSKIE": "Warmian-Masurian",
    "ŁÓDZKIE": "Łódź",
    "ŚWIĘTOKRZYSKIE": "Świętokrzyskie",
    "LUBELSKIE": "Lublin",
    "PODKARPACKIE": "Subcarpathian",
    "PODLASKIE": "Podlaskie",
    "MAZOWIECKIE": "Masovian",
}


def validate_azure_config():
    """Stops with a clear error when Azure credentials are unavailable."""
    missing = []
    if not STORAGE_ACCOUNT_NAME:
        missing.append("AZURE_STORAGE_ACCOUNT_NAME")
    if not STORAGE_ACCOUNT_KEY:
        missing.append("AZURE_STORAGE_ACCOUNT_KEY")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


def get_service_client():
    """Creates an authenticated Azure Data Lake Gen2 client."""
    validate_azure_config()
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(
        account_url=account_url,
        credential=STORAGE_ACCOUNT_KEY,
    )


def find_latest_raw_file(raw_file_system):
    """Finds the most recent complete GUS citizenship Raw file."""
    candidates = []

    for path in raw_file_system.get_paths(path=RAW_DIRECTORY):
        filename = Path(path.name).name

        if not path.is_directory and filename.startswith(RAW_FILE_PREFIX):
            candidates.append(path)

    if not candidates:
        raise FileNotFoundError(
            f"No file beginning with '{RAW_FILE_PREFIX}' was found in "
            f"container '{RAW_CONTAINER}/{RAW_DIRECTORY}'."
        )

    latest_file = max(
        candidates,
        key=lambda path: path.last_modified,
    )
    return latest_file.name


def download_raw_json(raw_file_system, raw_path):
    """Downloads and parses one JSON file directly from Azure raw."""
    print(f"Reading raw/{raw_path}...")
    file_client = raw_file_system.get_file_client(raw_path)
    raw_bytes = file_client.download_file().readall()
    return json.loads(raw_bytes.decode("utf-8"))


def flatten_citizenship_data(payload):
    """Converts the nested API response into one analytical row per value."""
    pipeline_extracted_at = payload.get("extractedAtUtc")
    output_rows = []

    for record in payload.get("results", []):
        original_territory = str(record.get("unitName", "")).strip()
        territory_name_en = TERRITORY_NAMES_EN.get(original_territory)

        if territory_name_en is None:
            raise ValueError(
                "Missing controlled English name for territory: "
                f"{original_territory!r}"
            )

        for value in record.get("values", []):
            output_rows.append(
                {
                    "source_system": "GUS BDL",
                    "source_dataset": "Census 2021 - citizenship",
                    "reference_year": value.get("year"),
                    "variable_id": record.get("variableId", record.get("id")),
                    "citizenship_name_en": str(record.get("citizenship", "")).strip(),
                    "citizenship_type": str(record.get("citizenshipType", "")).strip(),
                    "unit_id": str(record.get("unitId", "")).strip(),
                    "territory_name_original": original_territory,
                    "territory_name_en": territory_name_en,
                    "territory_level": record.get("unitLevel"),
                    "population": value.get("val"),
                    "source_last_update_utc": record.get("lastUpdate"),
                    "pipeline_extracted_at_utc": pipeline_extracted_at,
                }
            )

    dataframe = pd.DataFrame(output_rows)
    if dataframe.empty:
        raise ValueError("The raw payload did not contain statistical values.")

    dataframe["reference_year"] = pd.to_numeric(
        dataframe["reference_year"], errors="raise"
    ).astype("int64")
    dataframe["variable_id"] = pd.to_numeric(
        dataframe["variable_id"], errors="raise"
    ).astype("int64")
    dataframe["territory_level"] = pd.to_numeric(
        dataframe["territory_level"], errors="raise"
    ).astype("int64")
    dataframe["population"] = pd.to_numeric(
        dataframe["population"], errors="raise"
    ).astype("int64")

    timestamp_columns = [
        "source_last_update_utc",
        "pipeline_extracted_at_utc",
    ]
    for column in timestamp_columns:
        dataframe[column] = pd.to_datetime(dataframe[column], errors="raise", utc=True)

    string_columns = [
        "source_system",
        "source_dataset",
        "citizenship_name_en",
        "citizenship_type",
        "unit_id",
        "territory_name_original",
        "territory_name_en",
    ]
    for column in string_columns:
        dataframe[column] = dataframe[column].astype("string").str.strip()

    return dataframe.sort_values(
        ["reference_year", "unit_id", "variable_id"]
    ).reset_index(drop=True)


def validate_silver_data(dataframe, raw_total_records):
    """Runs structural and business reconciliation checks before upload."""
    errors = []
    key_columns = ["reference_year", "variable_id", "unit_id"]

    if len(dataframe) != raw_total_records:
        errors.append(
            f"Row count {len(dataframe)} does not match raw total "
            f"{raw_total_records}."
        )
    if dataframe[key_columns].duplicated().any():
        errors.append("Duplicate year-variable-territory keys were found.")
    if dataframe.isna().any().any():
        null_columns = dataframe.columns[dataframe.isna().any()].tolist()
        errors.append(f"Null values were found in: {null_columns}.")
    if (dataframe["population"] < 0).any():
        errors.append("Negative population values were found.")
    if set(dataframe["reference_year"]) != {EXPECTED_YEAR}:
        errors.append("Unexpected reference year was found.")
    if dataframe["unit_id"].nunique() != EXPECTED_TERRITORIES:
        errors.append("The dataset does not contain exactly 17 territories.")
    if dataframe["variable_id"].nunique() != EXPECTED_VARIABLES:
        errors.append("The dataset does not contain exactly 190 variables.")

    country_rows = dataframe[dataframe["citizenship_type"] == "country"]
    if country_rows["citizenship_name_en"].nunique() != EXPECTED_COUNTRIES:
        errors.append("The dataset does not contain exactly 187 countries.")

    national = dataframe[dataframe["territory_level"] == 0].set_index("variable_id")[
        "population"
    ]
    regional = (
        dataframe[dataframe["territory_level"] == 2]
        .groupby("variable_id")["population"]
        .sum()
    )
    if not national.sort_index().equals(regional.sort_index()):
        errors.append("At least one national value differs from its 16-region sum.")

    country_totals = country_rows.groupby("unit_id")["population"].sum().sort_index()
    aggregate_totals = (
        dataframe[dataframe["citizenship_type"] == "aggregate"]
        .set_index("unit_id")["population"]
        .sort_index()
    )
    if not country_totals.equals(aggregate_totals):
        errors.append("The sum of country rows differs from non-Polish citizenship.")

    if errors:
        raise ValueError("Silver validation failed:\n- " + "\n- ".join(errors))

    print("Validation passed:")
    print(f"  Rows: {len(dataframe)}")
    print(f"  Countries: {EXPECTED_COUNTRIES}")
    print(f"  Territories: {EXPECTED_TERRITORIES}")
    print("  National totals match all regional sums")
    print("  Country totals match non-Polish citizenship aggregates")


def save_silver_locally(dataframe):
    """Creates local CSV and Parquet copies for inspection."""
    output_directory = Path("data/silver/gus/census_2021/citizenship")
    output_directory.mkdir(parents=True, exist_ok=True)

    csv_path = output_directory / CSV_FILENAME
    parquet_path = output_directory / PARQUET_FILENAME

    dataframe.to_csv(csv_path, index=False, encoding="utf-8-sig")
    dataframe.to_parquet(parquet_path, index=False, engine="pyarrow")

    print(f"Saved locally: {csv_path}")
    print(f"Saved locally: {parquet_path}")
    return csv_path, parquet_path


def ensure_directory(file_system, directory_path):
    """Creates every level of a Data Lake directory path when absent."""
    current_path = ""
    for part in directory_path.split("/"):
        current_path = f"{current_path}/{part}" if current_path else part
        directory_client = file_system.get_directory_client(current_path)
        try:
            directory_client.create_directory()
        except ResourceExistsError:
            pass


def upload_silver_file(curated_file_system, local_path):
    """Uploads a local curated artifact to its Silver directory."""
    remote_path = f"{SILVER_DIRECTORY}/{local_path.name}"
    file_client = curated_file_system.get_file_client(remote_path)

    with open(local_path, "rb") as file:
        file_client.upload_data(file.read(), overwrite=True)

    print(f"Uploaded to curated/{remote_path}")


def write_run_manifest(curated_file_system, dataframe, raw_path):
    """Writes lightweight lineage and quality metadata for the Silver run."""
    manifest = {
        "sourcePath": f"{RAW_CONTAINER}/{raw_path}",
        "outputDirectory": f"{CURATED_CONTAINER}/{SILVER_DIRECTORY}",
        "processedAtUtc": datetime.now(timezone.utc).isoformat(),
        "referenceYear": EXPECTED_YEAR,
        "rowCount": len(dataframe),
        "countryCount": int(
            dataframe.loc[
                dataframe["citizenship_type"] == "country",
                "citizenship_name_en",
            ].nunique()
        ),
        "territoryCount": int(dataframe["unit_id"].nunique()),
        "validationStatus": "passed",
        "outputs": [CSV_FILENAME, PARQUET_FILENAME],
    }

    manifest_path = f"{SILVER_DIRECTORY}/_manifest.json"
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    curated_file_system.get_file_client(manifest_path).upload_data(
        manifest_bytes, overwrite=True
    )
    print(f"Uploaded to curated/{manifest_path}")


def main():
    """Executes the complete Raw-to-Silver transformation."""
    service_client = get_service_client()
    raw_file_system = service_client.get_file_system_client(RAW_CONTAINER)
    curated_file_system = service_client.get_file_system_client(CURATED_CONTAINER)

    raw_path = find_latest_raw_file(raw_file_system)
    payload = download_raw_json(raw_file_system, raw_path)

    print("Flattening and cleaning citizenship data...")
    silver_dataframe = flatten_citizenship_data(payload)
    validate_silver_data(
        silver_dataframe,
        raw_total_records=int(payload.get("totalRecords", 0)),
    )

    csv_path, parquet_path = save_silver_locally(silver_dataframe)

    ensure_directory(curated_file_system, SILVER_DIRECTORY)
    upload_silver_file(curated_file_system, csv_path)
    upload_silver_file(curated_file_system, parquet_path)
    write_run_manifest(curated_file_system, silver_dataframe, raw_path)

    print("Done. Raw-to-Silver transformation completed successfully.")


if __name__ == "__main__":
    main()
