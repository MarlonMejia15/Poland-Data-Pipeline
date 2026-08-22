"""
transform_udsc_residence_silver.py

Reads annual UdSC Excel workbooks from Azure Data Lake raw, dynamically
detects headers, standardizes inconsistent schemas, translates unique Polish
citizenship labels with Azure Translator, validates official totals, and writes
clean CSV/Parquet datasets to the curated Silver layer.
"""

import json
import os
import re
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv


load_dotenv()

STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
TRANSLATOR_KEY = os.getenv("TRANSLATOR_KEY")
TRANSLATOR_ENDPOINT = os.getenv(
    "TRANSLATOR_ENDPOINT",
    "https://api.cognitive.microsofttranslator.com",
)
TRANSLATOR_REGION = os.getenv("TRANSLATOR_REGION")

RAW_CONTAINER = "raw"
CURATED_CONTAINER = "curated"
RAW_MANIFEST_PATH = "udsc/annual/_manifest.json"
SILVER_DIRECTORY = "silver/udsc/residence/annual"
LOCAL_OUTPUT_DIRECTORY = Path("data/silver/udsc/residence/annual")
TRANSLATION_CACHE_PATH = Path("data/cache/udsc_country_translations.json")

APPLICATION_SHEETS = {
    "POB.CZASOWY-WNIOSKI": "temporary_residence",
    "POB.STAŁY-WNIOSKI": "permanent_residence",
    "REZYDENT-WNI": "long_term_eu_residence",
}

DECISION_SHEETS = {
    "POB.CZASOWY-DECYZJE": "temporary_residence",
    "POB.STAŁY-DECYZJE": "permanent_residence",
    "REZYDENT-DEC": "long_term_eu_residence",
}

DECISION_OUTCOME_ALIASES = {
    "positive": {"POZYTYWNA", "POZYTYWNE"},
    "negative": {"NEGATYWNA", "NEGATYWNE"},
    "discontinued": {"UMORZENIE", "UMORZENIA"},
    "left_without_consideration": {
        "POZOSTAWIENIE BEZ ROZPOZNANIA"
    },
}

DOCUMENT_TYPE_ALIASES = {
    "temporary_residence": {"POBYT CZASOWY"},
    "permanent_residence": {"POBYT STAŁY"},
    "long_term_eu_residence": {
        "POBYT REZYDENTA DŁUGOTERMINOWEGO UE/WE",
        "POBYT REZYDENTA DŁUGOTERMINOWEGO UE",
    },
    "eu_residence": {
        "ZAREJESTROWANIE POBYTU OB. UE",
        "POBYT OBYWATELA UE",
    },
    "eu_permanent_residence": {
        "POBYT STAŁY OBYWATELA UNII EUROPEJSKIEJ",
        "POBYT STAŁY OBYWATELA UE",
    },
    "eu_family_residence": {
        "POBYT CZŁONKA RODZINY OBYWATELA UNII EUROPEJSKIEJ",
        "POBYT CZŁONKA RODZINY OBYWATELA UE",
    },
    "eu_family_permanent_residence": {
        "POBYT STAŁY CZŁONKA RODZINY OBYWATELA UNII EUROPEJSKIEJ",
        "POBYT STAŁY CZŁONKA RODZINY OBYWATELA UE",
    },
    "uk_residence": {
        "PRAWO POBYTU OBYWATELA WB",
        "POBYT OBYWATELA WB",
    },
    "uk_permanent_residence": {
        "PRAWO STAŁEGO POBYTU OBYWATELA WB",
        "POBYT STALŁY OBYWATELA WB",
    },
    "uk_family_residence": {
        "PRAWO POBYTU CZŁONKA RODZINY OBYWATELA WB",
        "POBYT CZŁONKA RODZINY OBYWATELA WB",
    },
    "uk_family_permanent_residence": {
        "PRAWO STAŁEGO POBYTU CZŁONKA RODZINY OBYWATELA WB",
        "POBYT STAŁY CZŁONKA RODZINY OBYWATELA WB",
    },
    "asylum": {"AZYL"},
    "refugee_status": {"STATUS UCHODŹCY"},
    "subsidiary_protection": {"OCHRONA UZUPEŁNIAJĄCA"},
    "tolerated_stay": {"POBYT TOLEROWANY"},
    "humanitarian_stay": {
        "POBYT HUMNITARNY",
        "POBYT HUMANITARNY",
        "POBYT ZE WZGLĘDÓW HUMANITARNYCH",
    },
    "temporary_protection": {"OCHRONA CZASOWA"},
}

TOTAL_LABELS = {"SUMA", "RAZEM", "OGÓŁEM"}
SPECIAL_TRANSLATIONS = {
    "BEZ OBYWATELSTWA": "Stateless",
    "NIEOKREŚLONE": "Unknown",
    "NIEUSTALONE": "Unknown",
    "BRAK DANYCH": "Unknown",
}

EXPECTED_APPLICATION_TOTALS = {
    2021: 392_715,
    2022: 536_064,
    2023: 608_900,
    2024: 509_783,
    2025: 562_801,
}

EXPECTED_CORE_DOCUMENT_STOCK = {
    2021: 458_272,
    2022: 639_592,
    2023: 831_639,
    2024: 891_671,
    2025: 955_176,
}


def normalize_text(value):
    """Normalizes labels without depending on capitalization or spacing."""

    if value is None or pd.isna(value):
        return ""

    return " ".join(
        str(value).replace("\n", " ").strip().upper().split()
    )


def parse_count(value):
    """Converts Excel counts and source hyphens into integers."""

    if value is None or pd.isna(value):
        return 0

    if str(value).strip() in {"", "-"}:
        return 0

    return int(round(float(value)))


def get_file_system_clients():
    """Creates clients for the raw and curated containers."""

    if not STORAGE_ACCOUNT_NAME or not STORAGE_ACCOUNT_KEY:
        raise ValueError("Azure Storage credentials are missing from .env")

    account_url = (
        f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    )

    service_client = DataLakeServiceClient(
        account_url=account_url,
        credential=STORAGE_ACCOUNT_KEY,
    )

    return (
        service_client.get_file_system_client(RAW_CONTAINER),
        service_client.get_file_system_client(CURATED_CONTAINER),
    )


def download_bytes(file_system_client, path):
    """Downloads one Azure Data Lake file as bytes."""

    return (
        file_system_client.get_file_client(path)
        .download_file()
        .readall()
    )


def upload_bytes(file_system_client, path, data):
    """Uploads bytes to Azure Data Lake."""

    file_system_client.get_file_client(path).upload_data(
        data,
        overwrite=True,
    )


def load_source_workbooks(raw_client):
    """Uses the raw ingestion manifest to download every source workbook."""

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
            "bytes": download_bytes(raw_client, azure_path),
        }

    if set(workbooks) != set(EXPECTED_APPLICATION_TOTALS):
        raise ValueError(
            f"Unexpected source years: {sorted(workbooks)}"
        )

    return workbooks


def read_raw_sheet(workbook_bytes, sheet_name):
    """Reads an Excel sheet without assuming a header row."""

    return pd.read_excel(
        BytesIO(workbook_bytes),
        sheet_name=sheet_name,
        header=None,
        dtype=object,
        engine="openpyxl",
    )


def find_header_row(raw_dataframe):
    """Finds the row containing the citizenship header dynamically."""

    for row_index, row in raw_dataframe.iterrows():
        normalized_values = [normalize_text(value) for value in row]

        if "OBYWATELSTWO" in normalized_values:
            return row_index

    raise ValueError("Could not find OBYWATELSTWO header row")


def find_column(headers, accepted_names):
    """Finds a column by normalized header name."""

    for index, header in enumerate(headers):
        if header in accepted_names:
            return index

    raise ValueError(
        f"Could not find any of these columns: {accepted_names}"
    )


def is_percentage_total(value):
    """Detects a 100% grand-total row, including the mislabeled 2022 row."""

    if value is None or pd.isna(value):
        return False

    numeric_value = float(value)
    return abs(numeric_value - 100) < 0.1 or abs(numeric_value - 1) < 0.001


def extract_applications(
    workbook_bytes,
    filename,
    reference_year,
    sheet_name,
    permit_type,
):
    """Extracts one applications sheet by header names, not positions."""

    raw = read_raw_sheet(workbook_bytes, sheet_name)
    header_row = find_header_row(raw)
    headers = [normalize_text(value) for value in raw.iloc[header_row]]

    country_column = find_column(headers, {"OBYWATELSTWO"})
    total_column = find_column(headers, {"RAZEM", "SUMA"})
    percentage_column = next(
        index for index, header in enumerate(headers) if "%" in header
    )

    records = []
    official_total = None

    for _, row in raw.iloc[header_row + 1 :].iterrows():
        country = normalize_text(row.iloc[country_column])

        if not country:
            continue

        source_percentage = row.iloc[percentage_column]

        if country in TOTAL_LABELS or is_percentage_total(source_percentage):
            official_total = parse_count(row.iloc[total_column])

            if reference_year == 2022 and country == "SYRIA":
                print(
                    "  Corrected source anomaly: 2022 temporary "
                    "applications total was labeled SYRIA"
                )

            continue

        records.append(
            {
                "source_system": "UdSC",
                "reference_year": reference_year,
                "citizenship_name_pl": country,
                "permit_type": permit_type,
                "application_count": parse_count(
                    row.iloc[total_column]
                ),
                "source_file": filename,
                "source_sheet": sheet_name,
            }
        )

    extracted_total = sum(
        record["application_count"] for record in records
    )

    if official_total is None or extracted_total != official_total:
        raise ValueError(
            f"Applications validation failed for {reference_year} "
            f"{sheet_name}: extracted={extracted_total}, "
            f"official={official_total}"
        )

    return records


def extract_decisions(
    workbook_bytes,
    filename,
    reference_year,
    sheet_name,
    permit_type,
):
    """Extracts decision outcomes using normalized outcome names."""

    raw = read_raw_sheet(workbook_bytes, sheet_name)
    header_row = find_header_row(raw)
    outcome_headers = [
        normalize_text(value) for value in raw.iloc[header_row]
    ]
    subheaders = [
        normalize_text(value) for value in raw.iloc[header_row + 1]
    ]

    country_column = find_column(outcome_headers, {"OBYWATELSTWO"})
    outcome_columns = {}

    for canonical_outcome, aliases in DECISION_OUTCOME_ALIASES.items():
        start_column = find_column(outcome_headers, aliases)
        total_column = start_column + 2

        if subheaders[total_column] not in {"RAZEM", "SUMA"}:
            raise ValueError(
                f"Invalid total column for {reference_year} "
                f"{sheet_name} {canonical_outcome}"
            )

        outcome_columns[canonical_outcome] = total_column

    records = []
    official_totals = {}

    for _, row in raw.iloc[header_row + 2 :].iterrows():
        country = normalize_text(row.iloc[country_column])

        if not country:
            continue

        if country in TOTAL_LABELS:
            official_totals = {
                outcome: parse_count(row.iloc[column])
                for outcome, column in outcome_columns.items()
            }
            continue

        for outcome, column in outcome_columns.items():
            records.append(
                {
                    "source_system": "UdSC",
                    "reference_year": reference_year,
                    "citizenship_name_pl": country,
                    "permit_type": permit_type,
                    "decision_outcome": outcome,
                    "decision_count": parse_count(row.iloc[column]),
                    "source_file": filename,
                    "source_sheet": sheet_name,
                }
            )

    for outcome, official_total in official_totals.items():
        extracted_total = sum(
            record["decision_count"]
            for record in records
            if record["decision_outcome"] == outcome
        )

        if extracted_total != official_total:
            raise ValueError(
                f"Decisions validation failed for {reference_year} "
                f"{sheet_name} {outcome}: extracted={extracted_total}, "
                f"official={official_total}"
            )

    if set(official_totals) != set(outcome_columns):
        raise ValueError(
            f"Decision grand total was not found for {reference_year} "
            f"{sheet_name}"
        )

    return records


def build_document_alias_lookup():
    """Builds a normalized source-label to canonical-label dictionary."""

    return {
        alias: canonical_name
        for canonical_name, aliases in DOCUMENT_TYPE_ALIASES.items()
        for alias in aliases
    }


def extract_valid_documents(
    workbook_bytes,
    filename,
    reference_year,
):
    """Extracts document stock while allowing columns to change order."""

    sheet_name = "KARTY POBYTU"
    raw = read_raw_sheet(workbook_bytes, sheet_name)
    header_row = find_header_row(raw)
    headers = [normalize_text(value) for value in raw.iloc[header_row]]
    country_column = find_column(headers, {"OBYWATELSTWO"})
    alias_lookup = build_document_alias_lookup()

    document_columns = {
        index: alias_lookup[header]
        for index, header in enumerate(headers)
        if header in alias_lookup
    }

    ignored_headers = {"", "OBYWATELSTWO", "SUMA", "RAZEM"}
    unknown_headers = {
        header
        for header in headers
        if header not in ignored_headers and header not in alias_lookup
    }

    if unknown_headers:
        raise ValueError(
            f"Unknown KARTY POBYTU headers for {reference_year}: "
            f"{sorted(unknown_headers)}"
        )

    records = []
    official_totals = {}
    snapshot_date = f"{reference_year + 1}-01-01"

    for _, row in raw.iloc[header_row + 1 :].iterrows():
        country = normalize_text(row.iloc[country_column])

        if not country:
            continue

        if country in TOTAL_LABELS:
            official_totals = {
                document_type: parse_count(row.iloc[column])
                for column, document_type in document_columns.items()
            }
            continue

        for column, document_type in document_columns.items():
            records.append(
                {
                    "source_system": "UdSC",
                    "reference_year": reference_year,
                    "snapshot_date": snapshot_date,
                    "citizenship_name_pl": country,
                    "document_type": document_type,
                    "valid_document_count": parse_count(row.iloc[column]),
                    "source_file": filename,
                    "source_sheet": sheet_name,
                }
            )

    for document_type, official_total in official_totals.items():
        extracted_total = sum(
            record["valid_document_count"]
            for record in records
            if record["document_type"] == document_type
        )

        if extracted_total != official_total:
            raise ValueError(
                f"Document validation failed for {reference_year} "
                f"{document_type}: extracted={extracted_total}, "
                f"official={official_total}"
            )

    if set(official_totals) != set(document_columns.values()):
        raise ValueError(
            f"Document grand total was not found for {reference_year}"
        )

    return records


def load_translation_cache():
    """Loads previously translated values to avoid repeated API calls."""

    if not TRANSLATION_CACHE_PATH.exists():
        return {}

    with TRANSLATION_CACHE_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_translation_cache(cache):
    """Saves translations locally for reuse."""

    TRANSLATION_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

    with TRANSLATION_CACHE_PATH.open("w", encoding="utf-8") as file:
        json.dump(cache, file, ensure_ascii=False, indent=2)


def translate_country_names(country_names):
    """Translates only unique Polish citizenship labels in batches."""

    cache = load_translation_cache()
    cache.update(SPECIAL_TRANSLATIONS)

    missing_names = sorted(
        name for name in set(country_names) if name not in cache
    )

    if missing_names and not TRANSLATOR_KEY:
        raise ValueError("AZURE_TRANSLATOR_KEY is missing from .env")

    headers = {
        "Ocp-Apim-Subscription-Key": TRANSLATOR_KEY,
        "Content-Type": "application/json",
    }

    if TRANSLATOR_REGION:
        headers["Ocp-Apim-Subscription-Region"] = TRANSLATOR_REGION

    url = f"{TRANSLATOR_ENDPOINT.rstrip('/')}/translate"
    params = {"api-version": "3.0", "from": "pl", "to": "en"}

    for batch_start in range(0, len(missing_names), 50):
        batch = missing_names[batch_start : batch_start + 50]

        print(
            f"Translating citizenship labels "
            f"{batch_start + 1}-{batch_start + len(batch)} "
            f"of {len(missing_names)}..."
        )

        response = requests.post(
            url,
            params=params,
            headers=headers,
            json=[{"text": name.title()} for name in batch],
            timeout=60,
        )
        response.raise_for_status()

        translated_results = response.json()

        for source_name, result in zip(batch, translated_results):
            cache[source_name] = result["translations"][0]["text"]

        save_translation_cache(cache)

    save_translation_cache(cache)

    return cache


def enrich_country_names(dataframes):
    """Adds a common English citizenship label to every Silver table."""

    all_country_names = []

    for dataframe in dataframes:
        all_country_names.extend(dataframe["citizenship_name_pl"].tolist())

    translation_cache = translate_country_names(all_country_names)

    for dataframe in dataframes:
        dataframe.insert(
            dataframe.columns.get_loc("citizenship_name_pl") + 1,
            "citizenship_name_en",
            dataframe["citizenship_name_pl"].map(translation_cache),
        )

    return translation_cache


def validate_known_totals(applications, valid_documents):
    """Validates known cross-year totals to prevent silent schema errors."""

    application_totals = (
        applications.groupby("reference_year")["application_count"]
        .sum()
        .to_dict()
    )

    if application_totals != EXPECTED_APPLICATION_TOTALS:
        raise ValueError(
            f"Annual application totals differ: {application_totals}"
        )

    core_document_types = {
        "temporary_residence",
        "permanent_residence",
        "long_term_eu_residence",
    }

    stock_totals = (
        valid_documents[
            valid_documents["document_type"].isin(core_document_types)
        ]
        .groupby("reference_year")["valid_document_count"]
        .sum()
        .to_dict()
    )

    if stock_totals != EXPECTED_CORE_DOCUMENT_STOCK:
        raise ValueError(f"Annual document totals differ: {stock_totals}")


def save_and_upload_dataframe(
    dataframe,
    base_filename,
    curated_client,
):
    """Writes one Silver table as CSV and Parquet locally and in Azure."""

    LOCAL_OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)

    csv_path = LOCAL_OUTPUT_DIRECTORY / f"{base_filename}.csv"
    parquet_path = LOCAL_OUTPUT_DIRECTORY / f"{base_filename}.parquet"

    dataframe.to_csv(csv_path, index=False, encoding="utf-8-sig")
    dataframe.to_parquet(parquet_path, index=False)

    for local_path in (csv_path, parquet_path):
        azure_path = f"{SILVER_DIRECTORY}/{local_path.name}"
        upload_bytes(curated_client, azure_path, local_path.read_bytes())
        print(f"Uploaded to curated/{azure_path}")


def upload_silver_manifest(
    curated_client,
    applications,
    decisions,
    valid_documents,
    translation_cache,
    processed_at_utc,
):
    """Uploads Silver lineage and validation metadata."""

    manifest = {
        "source_system": "UdSC",
        "source_raw_manifest": f"{RAW_CONTAINER}/{RAW_MANIFEST_PATH}",
        "layer": "silver",
        "period_type": "annual",
        "years": sorted(EXPECTED_APPLICATION_TOTALS),
        "processed_at_utc": processed_at_utc,
        "row_counts": {
            "applications": len(applications),
            "decisions": len(decisions),
            "valid_documents": len(valid_documents),
        },
        "translated_unique_labels": len(translation_cache),
        "validation": {
            "source_sheet_totals_reconciled": True,
            "known_annual_totals_reconciled": True,
            "columns_mapped_by_normalized_name": True,
            "2022_mislabeled_total_handled": True,
        },
    }

    manifest_bytes = json.dumps(
        manifest,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    upload_bytes(
        curated_client,
        f"{SILVER_DIRECTORY}/_manifest.json",
        manifest_bytes,
    )

    upload_bytes(
        curated_client,
        f"{SILVER_DIRECTORY}/country_translation_cache.json",
        json.dumps(
            translation_cache,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8"),
    )


def main():
    """Runs the complete UdSC Raw-to-Silver transformation."""

    raw_client, curated_client = get_file_system_clients()
    workbooks = load_source_workbooks(raw_client)
    processed_at_utc = datetime.now(timezone.utc).isoformat()

    application_records = []
    decision_records = []
    document_records = []

    for year in sorted(workbooks):
        workbook = workbooks[year]
        print(f"\nProcessing UdSC annual report {year}...")

        for sheet_name, permit_type in APPLICATION_SHEETS.items():
            application_records.extend(
                extract_applications(
                    workbook["bytes"],
                    workbook["filename"],
                    year,
                    sheet_name,
                    permit_type,
                )
            )

        for sheet_name, permit_type in DECISION_SHEETS.items():
            decision_records.extend(
                extract_decisions(
                    workbook["bytes"],
                    workbook["filename"],
                    year,
                    sheet_name,
                    permit_type,
                )
            )

        document_records.extend(
            extract_valid_documents(
                workbook["bytes"],
                workbook["filename"],
                year,
            )
        )

    applications = pd.DataFrame(application_records)
    decisions = pd.DataFrame(decision_records)
    valid_documents = pd.DataFrame(document_records)

    enrich_country_names(
        [applications, decisions, valid_documents]
    )

    for dataframe in (applications, decisions, valid_documents):
        dataframe["pipeline_processed_at_utc"] = processed_at_utc

    validate_known_totals(applications, valid_documents)

    print("\nSilver validation passed:")
    print(f"  Applications: {len(applications):,} rows")
    print(f"  Decisions: {len(decisions):,} rows")
    print(f"  Valid documents: {len(valid_documents):,} rows")

    save_and_upload_dataframe(
        applications,
        "udsc_residence_applications_2021_2025",
        curated_client,
    )
    save_and_upload_dataframe(
        decisions,
        "udsc_residence_decisions_2021_2025",
        curated_client,
    )
    save_and_upload_dataframe(
        valid_documents,
        "udsc_valid_documents_2021_2025",
        curated_client,
    )

    translation_cache = load_translation_cache()

    upload_silver_manifest(
        curated_client,
        applications,
        decisions,
        valid_documents,
        translation_cache,
        processed_at_utc,
    )

    print("\nDone. UdSC Raw-to-Silver transformation completed.")


if __name__ == "__main__":
    main()