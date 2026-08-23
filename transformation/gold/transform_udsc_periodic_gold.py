"""
transform_udsc_periodic_gold.py

Adds H1 2025 and H1 2026 periods to the existing conformed dimensions and
builds national periodic residence facts for Power BI. Run this script after
the GUS Gold and annual UdSC Gold transformations.
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

CURATED_CONTAINER = "curated"
SCRIPT_VERSION = "2026-08-23.1"

SILVER_APPLICATIONS_PATH = (
    "silver/udsc/residence/periodic/"
    "udsc_residence_applications_h1_2025_2026.parquet"
)
SILVER_DECISIONS_PATH = (
    "silver/udsc/residence/periodic/"
    "udsc_residence_decisions_h1_2025_2026.parquet"
)

DIM_PERIOD_PATH = "gold/conformed/dimensions/dim_period_conformed.parquet"
DIM_SOURCE_PATH = "gold/conformed/dimensions/dim_source_conformed.parquet"
DIM_RESIDENCE_PATH = "gold/udsc/dimensions/dim_residence_type.parquet"
DIM_OUTCOME_PATH = "gold/udsc/dimensions/dim_decision_outcome.parquet"

CONFORMED_DIRECTORY = "gold/conformed/dimensions"
FACT_DIRECTORY = "gold/udsc/facts"
MANIFEST_PATH = "gold/udsc/_periodic_manifest.json"

LOCAL_CONFORMED_DIRECTORY = Path("data/gold/conformed/dimensions")
LOCAL_FACT_DIRECTORY = Path("data/gold/udsc/facts")

EXPECTED_APPLICATION_ROWS = 6
EXPECTED_DECISION_ROWS = 18
EXPECTED_APPLICATION_TOTALS = {2025: 378_756, 2026: 449_287}
EXPECTED_DECISION_TOTALS = {
    2025: {"positive": 187_539, "negative": 16_411, "discontinued": 7_151},
    2026: {"positive": 151_229, "negative": 15_125, "discontinued": 10_210},
}

SOURCE_URL = "https://www.gov.pl/web/udsc/raporty-okresowe2"
APPLICATION_SOURCE_NAME = "First-half residence applications"
DECISION_SOURCE_NAME = "First-half residence decisions"


def validate_credentials():
    """Checks that Azure Storage credentials exist."""

    if not STORAGE_ACCOUNT_NAME or not STORAGE_ACCOUNT_KEY:
        raise ValueError(
            "Missing AZURE_STORAGE_ACCOUNT_NAME or "
            "AZURE_STORAGE_ACCOUNT_KEY in .env"
        )


def get_file_system_client():
    """Creates the authenticated curated-container client."""

    validate_credentials()
    service_client = DataLakeServiceClient(
        account_url=(
            f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        ),
        credential=STORAGE_ACCOUNT_KEY,
    )
    return service_client.get_file_system_client(CURATED_CONTAINER)


def download_parquet(file_system_client, path):
    """Downloads one Parquet file into a DataFrame."""

    print(f"Reading curated/{path}...")
    data = (
        file_system_client.get_file_client(path)
        .download_file()
        .readall()
    )
    dataframe = pd.read_parquet(io.BytesIO(data), engine="pyarrow")
    if dataframe.empty:
        raise ValueError(f"Input table is empty: {path}")
    return dataframe


def load_inputs(file_system_client):
    """Loads periodic Silver facts and the existing shared dimensions."""

    return {
        "applications": download_parquet(
            file_system_client, SILVER_APPLICATIONS_PATH
        ),
        "decisions": download_parquet(
            file_system_client, SILVER_DECISIONS_PATH
        ),
        "dim_period": download_parquet(file_system_client, DIM_PERIOD_PATH),
        "dim_source": download_parquet(file_system_client, DIM_SOURCE_PATH),
        "dim_residence": download_parquet(
            file_system_client, DIM_RESIDENCE_PATH
        ),
        "dim_outcome": download_parquet(
            file_system_client, DIM_OUTCOME_PATH
        ),
    }


def extend_dim_period(dimension, applications):
    """Adds H1 period rows without treating them as complete years."""

    result = dimension.copy()
    if "period_start_date" not in result:
        result["period_start_date"] = pd.to_datetime(
            result["year"].astype(int).astype(str) + "-01-01"
        )
    else:
        result["period_start_date"] = pd.to_datetime(
            result["period_start_date"]
        )
    if "period_end_date" not in result:
        result["period_end_date"] = pd.to_datetime(
            result["year"].astype(int).astype(str) + "-12-31"
        )
    else:
        result["period_end_date"] = pd.to_datetime(result["period_end_date"])
    if "half_number" not in result:
        result["half_number"] = pd.NA
    if "is_complete_year" not in result:
        result["is_complete_year"] = True

    periodic_rows = (
        applications[
            [
                "period_key",
                "reference_year",
                "period_start_date",
                "period_end_date",
                "half_number",
                "is_complete_year",
            ]
        ]
        .drop_duplicates("period_key")
        .rename(columns={"reference_year": "year"})
    )
    periodic_rows["period_type"] = "Half Year"
    periodic_rows["period_label"] = (
        "H" + periodic_rows["half_number"].astype(str)
        + " " + periodic_rows["year"].astype(str)
    )
    periodic_rows["period_start_date"] = pd.to_datetime(
        periodic_rows["period_start_date"]
    )
    periodic_rows["period_end_date"] = pd.to_datetime(
        periodic_rows["period_end_date"]
    )

    result = result.loc[
        ~result["period_key"].isin(periodic_rows["period_key"])
    ]
    result = pd.concat([result, periodic_rows], ignore_index=True)
    result["period_key"] = result["period_key"].astype(int)
    result["year"] = result["year"].astype(int)
    return result.sort_values("period_key").reset_index(drop=True)


def extend_dim_source(dimension):
    """Adds separate source rows for the national H1 facts."""

    result = dimension.copy()
    additions = [
        {
            "source_system": "UdSC",
            "source_dataset": APPLICATION_SOURCE_NAME,
            "source_url": SOURCE_URL,
            "measure_name": "Residence applications",
            "measure_unit": "person",
            "source_granularity": "half-year national total by permit type",
        },
        {
            "source_system": "UdSC",
            "source_dataset": DECISION_SOURCE_NAME,
            "source_url": SOURCE_URL,
            "measure_name": "Residence decisions",
            "measure_unit": "person",
            "source_granularity": (
                "half-year national total by permit type and outcome"
            ),
        },
    ]

    for addition in additions:
        existing = result["source_dataset"] == addition["source_dataset"]
        if existing.any():
            continue
        addition["source_key"] = int(result["source_key"].max()) + 1
        result = pd.concat(
            [result, pd.DataFrame([addition])],
            ignore_index=True,
        )

    result["source_key"] = result["source_key"].astype(int)
    return result.sort_values("source_key").reset_index(drop=True)


def build_lookup_maps(dim_source, dim_residence, dim_outcome):
    """Creates mappings shared by both periodic facts."""

    source_map = dim_source.set_index("source_dataset")["source_key"]
    return {
        "application_source_key": int(source_map[APPLICATION_SOURCE_NAME]),
        "decision_source_key": int(source_map[DECISION_SOURCE_NAME]),
        "residence": dim_residence.set_index("residence_type_code")[
            "residence_type_key"
        ],
        "outcome": dim_outcome.set_index("decision_outcome_code")[
            "decision_outcome_key"
        ],
    }


def build_fact_applications(silver, lookup_maps):
    """Builds the national first-half applications fact."""

    fact = silver.copy()
    fact["residence_type_key"] = fact["permit_type"].map(
        lookup_maps["residence"]
    )
    fact["source_key"] = lookup_maps["application_source_key"]
    columns = [
        "period_key",
        "residence_type_key",
        "source_key",
        "application_count",
        "period_start_date",
        "period_end_date",
        "is_complete_year",
        "source_file",
        "source_sheet",
        "pipeline_processed_at_utc",
    ]
    fact = fact[columns].sort_values(
        ["period_key", "residence_type_key"]
    ).reset_index(drop=True)
    fact.insert(0, "periodic_application_key", range(1, len(fact) + 1))
    return fact


def build_fact_decisions(silver, lookup_maps):
    """Builds the national first-half decisions fact."""

    fact = silver.copy()
    fact["residence_type_key"] = fact["permit_type"].map(
        lookup_maps["residence"]
    )
    fact["decision_outcome_key"] = fact["decision_outcome"].map(
        lookup_maps["outcome"]
    )
    fact["source_key"] = lookup_maps["decision_source_key"]
    columns = [
        "period_key",
        "residence_type_key",
        "decision_outcome_key",
        "source_key",
        "decision_count",
        "period_start_date",
        "period_end_date",
        "is_complete_year",
        "source_file",
        "source_sheet",
        "pipeline_processed_at_utc",
    ]
    fact = fact[columns].sort_values(
        ["period_key", "residence_type_key", "decision_outcome_key"]
    ).reset_index(drop=True)
    fact.insert(0, "periodic_decision_key", range(1, len(fact) + 1))
    return fact


def validate_model(tables, silver_inputs):
    """Validates keys, grains, and the official H1 totals."""

    applications = tables["fact_residence_applications_h1"]
    decisions = tables["fact_residence_decisions_h1"]
    dim_period = tables["dim_period_conformed"]
    dim_source = tables["dim_source_conformed"]
    dim_residence = silver_inputs["dim_residence"]
    dim_outcome = silver_inputs["dim_outcome"]

    if len(applications) != EXPECTED_APPLICATION_ROWS:
        raise ValueError("Unexpected H1 application row count")
    if len(decisions) != EXPECTED_DECISION_ROWS:
        raise ValueError("Unexpected H1 decision row count")

    unique_checks = [
        (dim_period, "period_key"),
        (dim_source, "source_key"),
        (applications, "periodic_application_key"),
        (decisions, "periodic_decision_key"),
    ]
    for table, key in unique_checks:
        if table[key].isna().any() or table[key].duplicated().any():
            raise ValueError(f"Invalid key: {key}")

    foreign_key_checks = [
        (applications, "period_key", set(dim_period["period_key"])),
        (applications, "source_key", set(dim_source["source_key"])),
        (
            applications,
            "residence_type_key",
            set(dim_residence["residence_type_key"]),
        ),
        (decisions, "period_key", set(dim_period["period_key"])),
        (decisions, "source_key", set(dim_source["source_key"])),
        (
            decisions,
            "residence_type_key",
            set(dim_residence["residence_type_key"]),
        ),
        (
            decisions,
            "decision_outcome_key",
            set(dim_outcome["decision_outcome_key"]),
        ),
    ]
    for table, column, valid_values in foreign_key_checks:
        if table[column].isna().any() or not set(table[column]).issubset(
            valid_values
        ):
            raise ValueError(f"Broken foreign key: {column}")

    application_totals = (
        silver_inputs["applications"]
        .groupby("reference_year")["application_count"]
        .sum()
        .to_dict()
    )
    if application_totals != EXPECTED_APPLICATION_TOTALS:
        raise ValueError("H1 application totals do not match official values")

    decision_totals = (
        silver_inputs["decisions"]
        .groupby(["reference_year", "decision_outcome"])["decision_count"]
        .sum()
        .to_dict()
    )
    expected_decisions = {
        (year, outcome): count
        for year, outcomes in EXPECTED_DECISION_TOTALS.items()
        for outcome, count in outcomes.items()
    }
    if decision_totals != expected_decisions:
        raise ValueError("H1 decision totals do not match official values")

    if applications["is_complete_year"].any() or decisions[
        "is_complete_year"
    ].any():
        raise ValueError("H1 facts were incorrectly marked as complete years")

    print("Gold periodic validation passed:")
    print(f"  dim_period_conformed: {len(dim_period):,} rows")
    print(f"  dim_source_conformed: {len(dim_source):,} rows")
    print(f"  fact_residence_applications_h1: {len(applications):,} rows")
    print(f"  fact_residence_decisions_h1: {len(decisions):,} rows")
    print("  H1 2025 and H1 2026 official totals match")


def ensure_directory(file_system_client, directory_path):
    """Creates every Azure Data Lake directory level."""

    current_path = ""
    for part in directory_path.split("/"):
        current_path = f"{current_path}/{part}" if current_path else part
        try:
            file_system_client.get_directory_client(
                current_path
            ).create_directory()
        except ResourceExistsError:
            pass


def save_and_upload_table(
    file_system_client,
    table_name,
    dataframe,
    local_directory,
    azure_directory,
):
    """Writes one Gold table locally and uploads it as Parquet."""

    local_directory.mkdir(parents=True, exist_ok=True)
    ensure_directory(file_system_client, azure_directory)
    local_path = local_directory / f"{table_name}.parquet"
    dataframe.to_parquet(local_path, index=False, engine="pyarrow")
    remote_path = f"{azure_directory}/{local_path.name}"
    file_system_client.get_file_client(remote_path).upload_data(
        local_path.read_bytes(), overwrite=True
    )
    print(f"Uploaded to curated/{remote_path}")
    return f"curated/{remote_path}"


def save_and_upload_tables(file_system_client, tables):
    """Uploads the extended dimensions and two periodic facts."""

    output_paths = {}
    for table_name, dataframe in tables.items():
        if table_name in {"dim_period_conformed", "dim_source_conformed"}:
            local_directory = LOCAL_CONFORMED_DIRECTORY
            azure_directory = CONFORMED_DIRECTORY
        else:
            local_directory = LOCAL_FACT_DIRECTORY
            azure_directory = FACT_DIRECTORY
        output_paths[table_name] = save_and_upload_table(
            file_system_client,
            table_name,
            dataframe,
            local_directory,
            azure_directory,
        )
    return output_paths


def upload_manifest(file_system_client, tables, output_paths):
    """Uploads lineage and grain information for the periodic Gold facts."""

    manifest = {
        "script_version": SCRIPT_VERSION,
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        "periods": ["H1 2025", "H1 2026"],
        "is_complete_year": False,
        "source_sheet": "Arkusz11",
        "source_paths": {
            "applications": f"curated/{SILVER_APPLICATIONS_PATH}",
            "decisions": f"curated/{SILVER_DECISIONS_PATH}",
        },
        "grain": {
            "fact_residence_applications_h1": (
                "one national row per half-year and residence type"
            ),
            "fact_residence_decisions_h1": (
                "one national row per half-year, residence type, and outcome"
            ),
        },
        "citizenship_breakdown_available": False,
        "validation_status": "passed",
        "tables": {
            name: {
                "row_count": len(dataframe),
                "output_path": output_paths[name],
            }
            for name, dataframe in tables.items()
        },
    }
    ensure_directory(file_system_client, "gold/udsc")
    file_system_client.get_file_client(MANIFEST_PATH).upload_data(
        json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        overwrite=True,
    )
    print(f"Uploaded to curated/{MANIFEST_PATH}")


def main():
    """Executes the complete periodic UdSC Silver-to-Gold transformation."""

    print(f"UdSC periodic Gold transformer version: {SCRIPT_VERSION}")
    file_system_client = get_file_system_client()
    inputs = load_inputs(file_system_client)
    dim_period = extend_dim_period(
        inputs["dim_period"], inputs["applications"]
    )
    dim_source = extend_dim_source(inputs["dim_source"])
    lookup_maps = build_lookup_maps(
        dim_source,
        inputs["dim_residence"],
        inputs["dim_outcome"],
    )
    fact_applications = build_fact_applications(
        inputs["applications"], lookup_maps
    )
    fact_decisions = build_fact_decisions(inputs["decisions"], lookup_maps)

    tables = {
        "dim_period_conformed": dim_period,
        "dim_source_conformed": dim_source,
        "fact_residence_applications_h1": fact_applications,
        "fact_residence_decisions_h1": fact_decisions,
    }
    validate_model(tables, inputs)
    output_paths = save_and_upload_tables(file_system_client, tables)
    upload_manifest(file_system_client, tables, output_paths)
    print("Done. UdSC periodic Gold transformation completed successfully.")


if __name__ == "__main__":
    main()
