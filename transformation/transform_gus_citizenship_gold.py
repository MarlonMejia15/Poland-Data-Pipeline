"""
transform_citizenships_gold.py

Builds a Power BI-ready dimensional model from the validated GUS Census 2021
Silver citizenship dataset stored in Azure Data Lake Gen2.

Input:
    curated/silver/gus/census_2021/citizenship/
        gus_citizenships_2021.parquet

Outputs:
    curated/gold/dimensions/
        dim_country.parquet
        dim_geography.parquet
        dim_period.parquet
        dim_source.parquet
    curated/gold/facts/
        fact_citizenship_population.parquet
        fact_citizenship_summary.parquet

The main fact contains only real countries. Aggregate, stateless, and unknown
records are placed in a separate summary fact to prevent double counting.
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
SILVER_PATH = (
    "silver/gus/census_2021/citizenship/gus_citizenships_2021.parquet"
)
GOLD_DIMENSIONS_DIRECTORY = "gold/dimensions"
GOLD_FACTS_DIRECTORY = "gold/facts"
GOLD_MANIFEST_PATH = "gold/_manifest.json"
LOCAL_GOLD_DIRECTORY = Path("data") / "gold"

EXPECTED_COUNTRIES = 187
EXPECTED_TERRITORIES = 17
EXPECTED_COUNTRY_FACT_ROWS = 3179
EXPECTED_SUMMARY_FACT_ROWS = 51


# Normalizes misspellings and historical/common labels from the GUS catalog
# before country_converter assigns ISO codes and continents. The original GUS
# label is retained separately in dim_country.
COUNTRY_NAME_OVERRIDES = {
    "Andora": "Andorra",
    "Cape Verde": "Cabo Verde",
    "Congo": "Republic of the Congo",
    "Democratic Republic of the Congo": "DR Congo",
    "East Timor": "Timor-Leste",
    "Guinea Bissau": "Guinea-Bissau",
    "Ivory Coast": "Cote d'Ivoire",
    "Kyrgyztan": "Kyrgyzstan",
    "Micronesia": "Federated States of Micronesia",
    "New Zeland": "New Zealand",
    "North Korea": "North Korea",
    "Palestinian": "Palestine",
    "Paraguai": "Paraguay",
    "Quatar": "Qatar",
    "Salvador": "El Salvador",
    "Sao Tome and Principle": "Sao Tome and Principe",
    "South Korea": "South Korea",
    "Surinam": "Suriname",
    "Türkiye": "Turkey",
    "Vatican": "Vatican City",
}


def validate_azure_config():
    """Stops early when Azure credentials are not available."""
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
    """Creates the authenticated Azure Data Lake Gen2 service client."""
    validate_azure_config()
    return DataLakeServiceClient(
        account_url=(
            f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        ),
        credential=STORAGE_ACCOUNT_KEY,
    )


def download_silver_dataframe(curated_file_system):
    """Downloads the validated Silver Parquet dataset into a DataFrame."""
    print(f"Reading curated/{SILVER_PATH}...")
    parquet_bytes = (
        curated_file_system.get_file_client(SILVER_PATH)
        .download_file()
        .readall()
    )
    dataframe = pd.read_parquet(io.BytesIO(parquet_bytes), engine="pyarrow")

    if dataframe.empty:
        raise ValueError("The Silver citizenship dataset is empty.")
    return dataframe


def convert_country_attributes(country_names):
    """Returns standardized names, ISO codes, and continents."""
    try:
        import country_converter as coco
    except ImportError as error:
        raise RuntimeError(
            "Missing dependency 'country_converter'. Install it with: "
            "pip install country_converter"
        ) from error

    lookup_names = [
        COUNTRY_NAME_OVERRIDES.get(name, name) for name in country_names
    ]
    converter = coco.CountryConverter()
    missing_marker = "__NOT_FOUND__"

    standard_names = converter.convert(
        names=lookup_names,
        to="name_short",
        enforce_list=True,
        not_found=missing_marker,
    )
    iso2_codes = converter.convert(
        names=lookup_names,
        to="ISO2",
        enforce_list=True,
        not_found=missing_marker,
    )
    iso3_codes = converter.convert(
        names=lookup_names,
        to="ISO3",
        enforce_list=True,
        not_found=missing_marker,
    )
    continents = converter.convert(
        names=lookup_names,
        to="continent",
        enforce_list=True,
        not_found=missing_marker,
    )

    unresolved = [
        original
        for original, iso3 in zip(country_names, iso3_codes)
        if iso3 == missing_marker
    ]
    if unresolved:
        raise ValueError(
            "Country reference mapping was not found for: "
            + ", ".join(unresolved)
        )

    return standard_names, iso2_codes, iso3_codes, continents


def build_dim_country(silver):
    """Builds one conformed row for each real citizenship country."""
    country_names = sorted(
        silver.loc[
            silver["citizenship_type"] == "country",
            "citizenship_name_en",
        ].unique()
    )

    if len(country_names) != EXPECTED_COUNTRIES:
        raise ValueError(
            f"Expected {EXPECTED_COUNTRIES} countries, found "
            f"{len(country_names)}."
        )

    standard_names, iso2_codes, iso3_codes, continents = (
        convert_country_attributes(country_names)
    )
    dimension = pd.DataFrame(
        {
            "country_name_gus_en": country_names,
            "country_name_standard_en": standard_names,
            "iso2_code": iso2_codes,
            "iso3_code": iso3_codes,
            "continent": continents,
        }
    ).sort_values("iso3_code").reset_index(drop=True)

    if dimension["iso3_code"].duplicated().any():
        duplicates = dimension.loc[
            dimension["iso3_code"].duplicated(keep=False),
            ["country_name_gus_en", "iso3_code"],
        ]
        raise ValueError(
            "Duplicate ISO3 mappings were found:\n"
            + duplicates.to_string(index=False)
        )

    dimension.insert(0, "country_key", range(1, len(dimension) + 1))
    return dimension


def build_dim_geography(silver):
    """Builds the Poland-plus-voivodships geography dimension."""
    columns = [
        "unit_id",
        "territory_name_original",
        "territory_name_en",
        "territory_level",
    ]
    dimension = (
        silver[columns]
        .drop_duplicates()
        .sort_values(["territory_level", "unit_id"])
        .reset_index(drop=True)
    )

    if len(dimension) != EXPECTED_TERRITORIES:
        raise ValueError(
            f"Expected {EXPECTED_TERRITORIES} territories, found "
            f"{len(dimension)}."
        )

    dimension.insert(0, "geography_key", range(1, len(dimension) + 1))
    dimension["geography_type"] = dimension["territory_level"].map(
        {0: "Country", 2: "Voivodship"}
    )
    dimension["is_national_total"] = (
        dimension["territory_level"] == 0
    )
    return dimension


def build_dim_period(silver):
    """Builds annual analytical periods without inventing a precise date."""
    years = sorted(silver["reference_year"].astype(int).unique())
    dimension = pd.DataFrame(
        {
            "period_key": years,
            "year": years,
            "period_type": "Year",
            "period_label": [str(year) for year in years],
        }
    )
    return dimension


def build_dim_source(silver):
    """Documents the source system, dataset, and metric definition."""
    sources = (
        silver[["source_system", "source_dataset"]]
        .drop_duplicates()
        .sort_values(["source_system", "source_dataset"])
        .reset_index(drop=True)
    )
    sources.insert(0, "source_key", range(1, len(sources) + 1))
    sources["source_url"] = "https://bdl.stat.gov.pl/"
    sources["measure_name"] = "Usual resident population by citizenship"
    sources["measure_unit"] = "person"
    sources["source_granularity"] = "annual census snapshot"
    return sources


def build_fact_country_population(
    silver,
    dim_country,
    dim_geography,
    dim_source,
):
    """Builds the additive fact table containing only actual countries."""
    fact = silver[silver["citizenship_type"] == "country"].copy()
    fact = fact.merge(
        dim_country[["country_key", "country_name_gus_en"]],
        left_on="citizenship_name_en",
        right_on="country_name_gus_en",
        how="left",
        validate="many_to_one",
    )
    fact = fact.merge(
        dim_geography[["geography_key", "unit_id"]],
        on="unit_id",
        how="left",
        validate="many_to_one",
    )
    fact = fact.merge(
        dim_source[["source_key", "source_system", "source_dataset"]],
        on=["source_system", "source_dataset"],
        how="left",
        validate="many_to_one",
    )
    fact["period_key"] = fact["reference_year"].astype(int)

    output_columns = [
        "period_key",
        "country_key",
        "geography_key",
        "source_key",
        "population",
        "variable_id",
        "source_last_update_utc",
        "pipeline_extracted_at_utc",
    ]
    fact = fact[output_columns].sort_values(
        ["period_key", "geography_key", "country_key"]
    ).reset_index(drop=True)
    fact.insert(0, "citizenship_population_key", range(1, len(fact) + 1))
    return fact


def build_fact_summary(silver, dim_geography, dim_source):
    """Preserves non-country totals separately to prevent double counting."""
    fact = silver[silver["citizenship_type"] != "country"].copy()
    fact = fact.merge(
        dim_geography[["geography_key", "unit_id"]],
        on="unit_id",
        how="left",
        validate="many_to_one",
    )
    fact = fact.merge(
        dim_source[["source_key", "source_system", "source_dataset"]],
        on=["source_system", "source_dataset"],
        how="left",
        validate="many_to_one",
    )
    fact["period_key"] = fact["reference_year"].astype(int)

    fact = fact.rename(
        columns={
            "citizenship_name_en": "summary_name_en",
            "citizenship_type": "summary_type",
        }
    )
    output_columns = [
        "period_key",
        "geography_key",
        "source_key",
        "summary_name_en",
        "summary_type",
        "population",
        "variable_id",
        "source_last_update_utc",
        "pipeline_extracted_at_utc",
    ]
    fact = fact[output_columns].sort_values(
        ["period_key", "geography_key", "summary_type"]
    ).reset_index(drop=True)
    fact.insert(0, "citizenship_summary_key", range(1, len(fact) + 1))
    return fact


def validate_gold_model(tables):
    """Validates row counts, uniqueness, foreign keys, and totals."""
    dim_country = tables["dim_country"]
    dim_geography = tables["dim_geography"]
    dim_period = tables["dim_period"]
    dim_source = tables["dim_source"]
    fact_population = tables["fact_citizenship_population"]
    fact_summary = tables["fact_citizenship_summary"]
    errors = []

    expected_counts = {
        "dim_country": EXPECTED_COUNTRIES,
        "dim_geography": EXPECTED_TERRITORIES,
        "dim_period": 1,
        "dim_source": 1,
        "fact_citizenship_population": EXPECTED_COUNTRY_FACT_ROWS,
        "fact_citizenship_summary": EXPECTED_SUMMARY_FACT_ROWS,
    }
    for table_name, expected_count in expected_counts.items():
        actual_count = len(tables[table_name])
        if actual_count != expected_count:
            errors.append(
                f"{table_name}: expected {expected_count} rows, "
                f"found {actual_count}."
            )

    unique_keys = {
        "dim_country": "country_key",
        "dim_geography": "geography_key",
        "dim_period": "period_key",
        "dim_source": "source_key",
        "fact_citizenship_population": "citizenship_population_key",
        "fact_citizenship_summary": "citizenship_summary_key",
    }
    for table_name, key_column in unique_keys.items():
        table = tables[table_name]
        if table[key_column].isna().any() or table[key_column].duplicated().any():
            errors.append(f"{table_name}: invalid key {key_column}.")

    foreign_key_checks = [
        (
            fact_population,
            "country_key",
            set(dim_country["country_key"]),
            "fact population -> country",
        ),
        (
            fact_population,
            "geography_key",
            set(dim_geography["geography_key"]),
            "fact population -> geography",
        ),
        (
            fact_population,
            "period_key",
            set(dim_period["period_key"]),
            "fact population -> period",
        ),
        (
            fact_population,
            "source_key",
            set(dim_source["source_key"]),
            "fact population -> source",
        ),
        (
            fact_summary,
            "geography_key",
            set(dim_geography["geography_key"]),
            "fact summary -> geography",
        ),
        (
            fact_summary,
            "period_key",
            set(dim_period["period_key"]),
            "fact summary -> period",
        ),
        (
            fact_summary,
            "source_key",
            set(dim_source["source_key"]),
            "fact summary -> source",
        ),
    ]
    for fact, column, valid_values, label in foreign_key_checks:
        if not set(fact[column]).issubset(valid_values):
            errors.append(f"Broken foreign key: {label}.")

    population_by_geography = fact_population.groupby("geography_key")[
        "population"
    ].sum()
    aggregate_rows = fact_summary[
        fact_summary["summary_type"] == "aggregate"
    ].set_index("geography_key")["population"]
    if not population_by_geography.sort_index().equals(
        aggregate_rows.sort_index()
    ):
        errors.append(
            "Country fact totals differ from non-Polish summary totals."
        )

    if errors:
        raise ValueError("Gold validation failed:\n- " + "\n- ".join(errors))

    print("Gold validation passed:")
    for table_name, table in tables.items():
        print(f"  {table_name}: {len(table)} rows")
    print("  All surrogate and foreign keys are valid")
    print("  Country fact totals match non-Polish summary totals")


def ensure_directory(file_system, directory_path):
    """Creates each Azure Data Lake directory level when absent."""
    current_path = ""
    for part in directory_path.split("/"):
        current_path = f"{current_path}/{part}" if current_path else part
        directory_client = file_system.get_directory_client(current_path)
        try:
            directory_client.create_directory()
        except ResourceExistsError:
            pass


def save_and_upload_tables(curated_file_system, tables):
    """Writes every Gold table as Parquet locally and in Azure."""
    LOCAL_GOLD_DIRECTORY.mkdir(parents=True, exist_ok=True)
    ensure_directory(curated_file_system, GOLD_DIMENSIONS_DIRECTORY)
    ensure_directory(curated_file_system, GOLD_FACTS_DIRECTORY)

    dimension_names = {
        "dim_country",
        "dim_geography",
        "dim_period",
        "dim_source",
    }
    output_paths = {}

    for table_name, dataframe in tables.items():
        local_path = LOCAL_GOLD_DIRECTORY / f"{table_name}.parquet"
        dataframe.to_parquet(local_path, index=False, engine="pyarrow")

        directory = (
            GOLD_DIMENSIONS_DIRECTORY
            if table_name in dimension_names
            else GOLD_FACTS_DIRECTORY
        )
        remote_path = f"{directory}/{local_path.name}"
        with open(local_path, "rb") as file:
            curated_file_system.get_file_client(remote_path).upload_data(
                file.read(), overwrite=True
            )

        output_paths[table_name] = f"{CURATED_CONTAINER}/{remote_path}"
        print(f"Uploaded to {CURATED_CONTAINER}/{remote_path}")

    return output_paths


def write_gold_manifest(curated_file_system, tables, output_paths):
    """Writes lineage, grain, row counts, and validation metadata."""
    manifest = {
        "sourcePath": f"{CURATED_CONTAINER}/{SILVER_PATH}",
        "processedAtUtc": datetime.now(timezone.utc).isoformat(),
        "modelType": "fact constellation with shared star dimensions",
        "validationStatus": "passed",
        "tables": {
            name: {
                "rowCount": len(dataframe),
                "outputPath": output_paths[name],
            }
            for name, dataframe in tables.items()
        },
        "grain": {
            "fact_citizenship_population": (
                "one row per year, real citizenship country, and territory"
            ),
            "fact_citizenship_summary": (
                "one row per year, non-country summary type, and territory"
            ),
        },
        "doubleCountingProtection": (
            "aggregate, stateless, and unknown rows are excluded from the "
            "country-level fact"
        ),
    }
    manifest_bytes = json.dumps(
        manifest, ensure_ascii=False, indent=2
    ).encode("utf-8")
    curated_file_system.get_file_client(GOLD_MANIFEST_PATH).upload_data(
        manifest_bytes, overwrite=True
    )
    print(f"Uploaded to {CURATED_CONTAINER}/{GOLD_MANIFEST_PATH}")


def main():
    """Executes the complete Silver-to-Gold dimensional transformation."""
    service_client = get_service_client()
    curated_file_system = service_client.get_file_system_client(
        CURATED_CONTAINER
    )
    silver = download_silver_dataframe(curated_file_system)

    print("Building Gold dimensions...")
    dim_country = build_dim_country(silver)
    dim_geography = build_dim_geography(silver)
    dim_period = build_dim_period(silver)
    dim_source = build_dim_source(silver)

    print("Building Gold facts...")
    fact_population = build_fact_country_population(
        silver,
        dim_country,
        dim_geography,
        dim_source,
    )
    fact_summary = build_fact_summary(
        silver,
        dim_geography,
        dim_source,
    )

    tables = {
        "dim_country": dim_country,
        "dim_geography": dim_geography,
        "dim_period": dim_period,
        "dim_source": dim_source,
        "fact_citizenship_population": fact_population,
        "fact_citizenship_summary": fact_summary,
    }
    validate_gold_model(tables)
    output_paths = save_and_upload_tables(curated_file_system, tables)
    write_gold_manifest(curated_file_system, tables, output_paths)

    print("Done. Silver-to-Gold transformation completed successfully.")


if __name__ == "__main__":
    main()