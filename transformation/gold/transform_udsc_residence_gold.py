"""
transform_udsc_residence_gold.py

Builds a Power BI-ready Gold model from the validated UdSC Silver datasets.
The model preserves the existing GUS country keys, extends the country
dimension with new ISO entities, and adds a citizenship-label dimension so
multiple source labels can map safely to the same country.

Inputs:
    curated/silver/udsc/residence/annual/*.parquet
    curated/gold/dimensions/dim_country.parquet
    curated/gold/dimensions/dim_period.parquet
    curated/gold/dimensions/dim_source.parquet

Outputs:
    curated/gold/conformed/dimensions/*.parquet
    curated/gold/udsc/dimensions/*.parquet
    curated/gold/udsc/facts/*.parquet
    curated/gold/udsc/_manifest.json
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
SCRIPT_VERSION = "2026-08-22.5"

SILVER_APPLICATIONS_PATH = (
    "silver/udsc/residence/annual/"
    "udsc_residence_applications_2021_2025.parquet"
)
SILVER_DECISIONS_PATH = (
    "silver/udsc/residence/annual/"
    "udsc_residence_decisions_2021_2025.parquet"
)
SILVER_DOCUMENTS_PATH = (
    "silver/udsc/residence/annual/"
    "udsc_valid_documents_2021_2025.parquet"
)

GUS_DIM_COUNTRY_PATH = "gold/gus/dimensions/dim_country.parquet"
GUS_DIM_PERIOD_PATH = "gold/gus/dimensions/dim_period.parquet"
GUS_DIM_SOURCE_PATH = "gold/gus/dimensions/dim_source.parquet"

CONFORMED_DIMENSIONS_DIRECTORY = "gold/conformed/dimensions"
UDSC_DIMENSIONS_DIRECTORY = "gold/udsc/dimensions"
UDSC_FACTS_DIRECTORY = "gold/udsc/facts"
UDSC_MANIFEST_PATH = "gold/udsc/_manifest.json"

LOCAL_CONFORMED_DIRECTORY = Path("data/gold/conformed/dimensions")
LOCAL_UDSC_DIMENSIONS_DIRECTORY = Path("data/gold/udsc/dimensions")
LOCAL_UDSC_FACTS_DIRECTORY = Path("data/gold/udsc/facts")

EXPECTED_APPLICATION_ROWS = 1_682
EXPECTED_DECISION_ROWS = 6_616
EXPECTED_DOCUMENT_ROWS = 15_226

EXPECTED_APPLICATION_TOTALS = {
    2021: 392_715,
    2022: 536_064,
    2023: 608_900,
    2024: 509_783,
    2025: 562_801,
}

EXPECTED_NEGATIVE_DECISIONS = {
    2021: 40_342,
    2022: 36_293,
    2023: 28_043,
    2024: 34_691,
    2025: 31_812,
}

EXPECTED_CORE_DOCUMENT_STOCK = {
    2021: 458_272,
    2022: 639_592,
    2023: 831_639,
    2024: 891_671,
    2025: 955_176,
}


# Controlled Gold standardization. Silver retains Translator output exactly.
COUNTRY_NAME_OVERRIDES = {
    "Small": "Mali",
    "Saint Christopher I Newis": "Saint Kitts and Nevis",
    "Viet Nam": "Vietnam",
    "U.A.E": "United Arab Emirates",
    "Western Samoa": "Samoa",
    "Swaziland": "Eswatini",
    "Macau S.A.R": "Macao",
    "Cape Verde": "Cabo Verde",
    "Czech Republic": "Czechia",
    "The Gambia": "Gambia",
    "Congo": "Republic of the Congo",
    "Democratic Republic of the Congo": "DR Congo",
    "Micronesia": "Federated States of Micronesia",
    "United States of America": "United States",
    "Côte d'Ivoire": "Cote d'Ivoire",
    "São Tomé and Príncipe": "Sao Tome and Principe",
}

SPECIAL_CITIZENSHIP_TYPES = {
    "Stateless": "stateless",
    "Unknown": "unknown",
    "Serbia and Montenegro": "historical_entity",
}

# Explicit attributes for politically or technically difficult ISO mappings.
MANUAL_ENTITY_ATTRIBUTES = {
    "Kosovo": {
        "country_name_standard_en": "Kosovo",
        "iso2_code": "XK",
        "iso3_code": "XKX",
        "continent": "Europe",
    },
    "Palestine": {
        "country_name_standard_en": "Palestine",
        "iso2_code": "PS",
        "iso3_code": "PSE",
        "continent": "Asia",
    },
    "Taiwan": {
        "country_name_standard_en": "Taiwan",
        "iso2_code": "TW",
        "iso3_code": "TWN",
        "continent": "Asia",
    },
    "Macao": {
        "country_name_standard_en": "Macao",
        "iso2_code": "MO",
        "iso3_code": "MAC",
        "continent": "Asia",
    },
    "Hong Kong": {
        "country_name_standard_en": "Hong Kong",
        "iso2_code": "HK",
        "iso3_code": "HKG",
        "continent": "Asia",
    },
}

LATIN_AMERICA_CARIBBEAN_ISO3 = {
    "AIA", "ARG", "ABW", "ATG", "BHS", "BLZ", "BOL", "BRA",
    "BRB", "CHL", "COL", "CRI", "CUB", "DMA", "DOM", "ECU",
    "GRD", "GTM", "GUY", "HND", "HTI", "JAM", "KNA", "LCA",
    "MEX", "NIC", "PAN", "PER", "PRI", "PRY", "SLV", "SUR",
    "TTO", "URY", "VCT", "VEN",
}

RESIDENCE_TYPES = [
    {
        "residence_type_code": "temporary_residence",
        "residence_type_name_en": "Temporary residence",
        "residence_category": "Ordinary residence",
    },
    {
        "residence_type_code": "permanent_residence",
        "residence_type_name_en": "Permanent residence",
        "residence_category": "Ordinary residence",
    },
    {
        "residence_type_code": "long_term_eu_residence",
        "residence_type_name_en": "EU long-term resident",
        "residence_category": "Ordinary residence",
    },
    {
        "residence_type_code": "eu_residence",
        "residence_type_name_en": "EU citizen residence",
        "residence_category": "EU free movement",
    },
    {
        "residence_type_code": "eu_permanent_residence",
        "residence_type_name_en": "EU citizen permanent residence",
        "residence_category": "EU free movement",
    },
    {
        "residence_type_code": "eu_family_residence",
        "residence_type_name_en": "EU citizen family residence",
        "residence_category": "EU free movement",
    },
    {
        "residence_type_code": "eu_family_permanent_residence",
        "residence_type_name_en": (
            "EU citizen family permanent residence"
        ),
        "residence_category": "EU free movement",
    },
    {
        "residence_type_code": "uk_residence",
        "residence_type_name_en": "UK citizen residence",
        "residence_category": "UK Withdrawal Agreement",
    },
    {
        "residence_type_code": "uk_permanent_residence",
        "residence_type_name_en": "UK citizen permanent residence",
        "residence_category": "UK Withdrawal Agreement",
    },
    {
        "residence_type_code": "uk_family_residence",
        "residence_type_name_en": "UK citizen family residence",
        "residence_category": "UK Withdrawal Agreement",
    },
    {
        "residence_type_code": "uk_family_permanent_residence",
        "residence_type_name_en": (
            "UK citizen family permanent residence"
        ),
        "residence_category": "UK Withdrawal Agreement",
    },
    {
        "residence_type_code": "asylum",
        "residence_type_name_en": "Asylum",
        "residence_category": "International protection",
    },
    {
        "residence_type_code": "refugee_status",
        "residence_type_name_en": "Refugee status",
        "residence_category": "International protection",
    },
    {
        "residence_type_code": "subsidiary_protection",
        "residence_type_name_en": "Subsidiary protection",
        "residence_category": "International protection",
    },
    {
        "residence_type_code": "tolerated_stay",
        "residence_type_name_en": "Tolerated stay",
        "residence_category": "Humanitarian and tolerated stay",
    },
    {
        "residence_type_code": "humanitarian_stay",
        "residence_type_name_en": "Humanitarian stay",
        "residence_category": "Humanitarian and tolerated stay",
    },
    {
        "residence_type_code": "temporary_protection",
        "residence_type_name_en": "Temporary protection",
        "residence_category": "Temporary protection",
    },
]

DECISION_OUTCOMES = [
    ("positive", "Positive"),
    ("negative", "Negative"),
    ("discontinued", "Discontinued"),
    ("left_without_consideration", "Left without consideration"),
]


def validate_azure_config():
    """Stops early if Azure Storage credentials are unavailable."""

    missing = []
    if not STORAGE_ACCOUNT_NAME:
        missing.append("AZURE_STORAGE_ACCOUNT_NAME")
    if not STORAGE_ACCOUNT_KEY:
        missing.append("AZURE_STORAGE_ACCOUNT_KEY")

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


def get_curated_file_system():
    """Creates the authenticated curated-container client."""

    validate_azure_config()
    service_client = DataLakeServiceClient(
        account_url=(
            f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
        ),
        credential=STORAGE_ACCOUNT_KEY,
    )
    return service_client.get_file_system_client(CURATED_CONTAINER)


def download_parquet(file_system, path):
    """Downloads one Parquet file into a DataFrame."""

    print(f"Reading {CURATED_CONTAINER}/{path}...")
    file_bytes = (
        file_system.get_file_client(path).download_file().readall()
    )
    dataframe = pd.read_parquet(io.BytesIO(file_bytes), engine="pyarrow")

    if dataframe.empty:
        raise ValueError(f"Input table is empty: {path}")

    return dataframe


def load_inputs(file_system):
    """Downloads Silver facts and the existing GUS base dimensions."""

    return {
        "applications": download_parquet(
            file_system, SILVER_APPLICATIONS_PATH
        ),
        "decisions": download_parquet(
            file_system, SILVER_DECISIONS_PATH
        ),
        "documents": download_parquet(
            file_system, SILVER_DOCUMENTS_PATH
        ),
        "gus_dim_country": download_parquet(
            file_system, GUS_DIM_COUNTRY_PATH
        ),
        "gus_dim_period": download_parquet(
            file_system, GUS_DIM_PERIOD_PATH
        ),
        "gus_dim_source": download_parquet(
            file_system, GUS_DIM_SOURCE_PATH
        ),
    }


def collect_citizenship_labels(inputs):
    """Returns every unique Polish-English UdSC citizenship label pair."""

    label_frames = []

    for table_name in ("applications", "decisions", "documents"):
        label_frames.append(
            inputs[table_name][
                ["citizenship_name_pl", "citizenship_name_en"]
            ]
        )

    labels = (
        pd.concat(label_frames, ignore_index=True)
        .drop_duplicates()
        .sort_values(["citizenship_name_pl", "citizenship_name_en"])
        .reset_index(drop=True)
    )

    duplicated_polish = labels["citizenship_name_pl"].duplicated(keep=False)
    if duplicated_polish.any():
        raise ValueError(
            "One Polish citizenship label has multiple English translations:\n"
            + labels.loc[duplicated_polish].to_string(index=False)
        )

    return labels


def canonicalize_country_name(translated_name):
    """Applies controlled corrections without changing Silver values."""

    return COUNTRY_NAME_OVERRIDES.get(translated_name, translated_name)


def scalarize_country_attribute(value):
    """Converts legacy one-item lists/arrays into scalar text values."""

    if hasattr(value, "tolist") and not isinstance(value, str):
        value = value.tolist()

    while isinstance(value, (list, tuple)):
        if len(value) != 1:
            raise ValueError(
                "Expected one country attribute value, found: "
                f"{value}"
            )
        value = value[0]

    return value


def resolve_converter_values(source, standard, iso2, iso3, continent):
    """Selects the exact country when coco returns multiple regex matches."""

    raw_values = {
        "country_name_standard_en": standard,
        "iso2_code": iso2,
        "iso3_code": iso3,
        "continent": continent,
    }
    list_lengths = {
        len(value)
        for value in raw_values.values()
        if isinstance(value, list)
    }
    if not list_lengths:
        return raw_values

    if len(list_lengths) != 1:
        raise ValueError(
            f"Inconsistent country_converter matches for {source}: "
            f"{raw_values}"
        )

    match_count = list_lengths.pop()
    candidates = []
    for index in range(match_count):
        candidate = {
            attribute: (
                value[index] if isinstance(value, list) else value
            )
            for attribute, value in raw_values.items()
        }
        candidates.append(candidate)

    normalized_source = source.casefold().strip()
    exact_matches = [
        candidate
        for candidate in candidates
        if str(candidate["country_name_standard_en"]).casefold().strip()
        == normalized_source
    ]
    if len(exact_matches) == 1:
        print(
            "Resolved multiple country_converter matches for "
            f"{source} using the exact standard name"
        )
        return exact_matches[0]

    raise ValueError(
        f"Ambiguous country reference mapping for {source}: {candidates}"
    )


def convert_country_attributes(canonical_names):
    """Converts standardized names to ISO and continent attributes."""

    try:
        import country_converter as coco
    except ImportError as error:
        raise RuntimeError(
            "Missing dependency 'country_converter'. Install it with: "
            "pip install country_converter"
        ) from error

    converter = coco.CountryConverter()
    missing_marker = "__NOT_FOUND__"
    output = {}
    automatic_names = [
        name
        for name in canonical_names
        if name not in MANUAL_ENTITY_ATTRIBUTES
    ]

    if automatic_names:
        standard_names = converter.convert(
            names=automatic_names,
            to="name_short",
            enforce_list=False,
            not_found=missing_marker,
        )
        iso2_codes = converter.convert(
            names=automatic_names,
            to="ISO2",
            enforce_list=False,
            not_found=missing_marker,
        )
        iso3_codes = converter.convert(
            names=automatic_names,
            to="ISO3",
            enforce_list=False,
            not_found=missing_marker,
        )
        continents = converter.convert(
            names=automatic_names,
            to="continent",
            enforce_list=False,
            not_found=missing_marker,
        )

        unresolved = []
        for source, standard, iso2, iso3, continent in zip(
            automatic_names,
            standard_names,
            iso2_codes,
            iso3_codes,
            continents,
        ):
            converted_values = resolve_converter_values(
                source, standard, iso2, iso3, continent
            )
            standard = converted_values["country_name_standard_en"]
            iso2 = converted_values["iso2_code"]
            iso3 = converted_values["iso3_code"]
            continent = converted_values["continent"]

            if missing_marker in {standard, iso2, iso3, continent}:
                unresolved.append(source)
                continue

            output[source] = {
                "country_name_standard_en": standard,
                "iso2_code": iso2,
                "iso3_code": iso3,
                "continent": continent,
            }

        if unresolved:
            raise ValueError(
                "Country reference mapping was not found for: "
                + ", ".join(sorted(unresolved))
            )

    output.update(MANUAL_ENTITY_ATTRIBUTES)
    return output


def build_country_reference(labels):
    """Classifies labels and maps real geopolitical entities to ISO."""

    reference = labels.copy()
    reference["country_name_canonical_en"] = reference[
        "citizenship_name_en"
    ].map(canonicalize_country_name)
    reference["citizenship_type"] = reference[
        "country_name_canonical_en"
    ].map(SPECIAL_CITIZENSHIP_TYPES).fillna("country_or_territory")

    real_entities = sorted(
        reference.loc[
            reference["citizenship_type"] == "country_or_territory",
            "country_name_canonical_en",
        ].unique()
    )
    attributes = convert_country_attributes(real_entities)

    for column in (
        "country_name_standard_en",
        "iso2_code",
        "iso3_code",
        "continent",
    ):
        reference[column] = reference["country_name_canonical_en"].map(
            {
                name: values[column] for name, values in attributes.items()
            }
        )

    reference["is_latin_america_caribbean"] = reference[
        "iso3_code"
    ].isin(LATIN_AMERICA_CARIBBEAN_ISO3)
    return reference


def build_conformed_dim_country(gus_dim_country, country_reference):
    """Preserves GUS keys and appends new UdSC ISO entities."""

    dimension = gus_dim_country.copy()
    required_columns = {
        "country_key",
        "country_name_gus_en",
        "country_name_standard_en",
        "iso2_code",
        "iso3_code",
        "continent",
    }

    missing_columns = required_columns - set(dimension.columns)
    if missing_columns:
        raise ValueError(
            f"GUS dim_country is missing: {sorted(missing_columns)}"
        )

    # The original GUS Gold pipeline used country_converter with
    # enforce_list=True. Parquet therefore preserved some ISO attributes as
    # one-item NumPy arrays. Normalize those inherited values without changing
    # the existing GUS surrogate keys.
    for column in (
        "country_name_standard_en",
        "iso2_code",
        "iso3_code",
        "continent",
    ):
        dimension[column] = dimension[column].map(
            scalarize_country_attribute
        )

    base_dimension = dimension.copy()

    dimension["is_latin_america_caribbean"] = dimension[
        "iso3_code"
    ].isin(LATIN_AMERICA_CARIBBEAN_ISO3)
    existing_iso3 = set(dimension["iso3_code"])

    new_entities = (
        country_reference.loc[
            country_reference["citizenship_type"]
            == "country_or_territory",
            [
                "country_name_standard_en",
                "iso2_code",
                "iso3_code",
                "continent",
                "is_latin_america_caribbean",
            ],
        ]
        .drop_duplicates("iso3_code")
        .loc[lambda frame: ~frame["iso3_code"].isin(existing_iso3)]
        .sort_values("iso3_code")
        .reset_index(drop=True)
    )

    if not new_entities.empty:
        next_key = int(dimension["country_key"].max()) + 1
        new_entities.insert(
            0,
            "country_key",
            range(next_key, next_key + len(new_entities)),
        )
        new_entities.insert(1, "country_name_gus_en", pd.NA)
        dimension = pd.concat(
            [dimension, new_entities],
            ignore_index=True,
        )

    dimension = dimension.sort_values("country_key").reset_index(drop=True)

    if dimension["country_key"].duplicated().any():
        raise ValueError("Duplicate country surrogate keys were found.")
    if dimension["iso3_code"].duplicated().any():
        raise ValueError("Duplicate ISO3 country mappings were found.")

    base_key_map = base_dimension.set_index("iso3_code")["country_key"]
    conformed_key_map = dimension.set_index("iso3_code")["country_key"]
    if not base_key_map.equals(conformed_key_map.loc[base_key_map.index]):
        raise ValueError("Existing GUS country keys were changed.")

    return dimension


def build_dim_citizenship(country_reference, dim_country):
    """Maps every source citizenship label to a conformed country key."""

    country_key_map = dim_country.set_index("iso3_code")["country_key"]
    dimension = country_reference.copy()
    dimension["country_key"] = dimension["iso3_code"].map(country_key_map)
    dimension = dimension.sort_values("citizenship_name_pl").reset_index(
        drop=True
    )
    dimension.insert(0, "citizenship_key", range(1, len(dimension) + 1))

    real_rows = dimension[
        dimension["citizenship_type"] == "country_or_territory"
    ]
    special_rows = dimension[
        dimension["citizenship_type"] != "country_or_territory"
    ]

    if real_rows["country_key"].isna().any():
        raise ValueError("A real citizenship label lacks a country key.")
    if special_rows["country_key"].notna().any():
        raise ValueError("A special citizenship label has a country key.")

    output_columns = [
        "citizenship_key",
        "country_key",
        "citizenship_name_pl",
        "citizenship_name_en",
        "country_name_canonical_en",
        "citizenship_type",
        "iso2_code",
        "iso3_code",
        "continent",
        "is_latin_america_caribbean",
    ]
    return dimension[output_columns]


def build_conformed_dim_period(gus_dim_period):
    """Preserves existing period keys and adds 2022-2026."""

    dimension = gus_dim_period.copy()
    existing_years = set(dimension["year"].astype(int))
    required_years = set(range(2021, 2027))

    new_rows = pd.DataFrame(
        {
            "period_key": sorted(required_years - existing_years),
            "year": sorted(required_years - existing_years),
            "period_type": "Year",
            "period_label": [
                str(year) for year in sorted(required_years - existing_years)
            ],
        }
    )

    dimension = pd.concat([dimension, new_rows], ignore_index=True)
    dimension["period_key"] = dimension["period_key"].astype(int)
    dimension["year"] = dimension["year"].astype(int)
    return dimension.sort_values("period_key").reset_index(drop=True)


def build_conformed_dim_source(gus_dim_source):
    """Preserves the GUS source and appends three UdSC measures."""

    dimension = gus_dim_source.copy()
    next_key = int(dimension["source_key"].max()) + 1
    udsc_sources = pd.DataFrame(
        [
            {
                "source_key": next_key,
                "source_system": "UdSC",
                "source_dataset": "Annual residence applications",
                "source_url": "https://www.gov.pl/web/udsc/zestawienia-roczne",
                "measure_name": "Residence applications",
                "measure_unit": "person",
                "source_granularity": "annual by citizenship and permit type",
            },
            {
                "source_key": next_key + 1,
                "source_system": "UdSC",
                "source_dataset": "Annual residence decisions",
                "source_url": "https://www.gov.pl/web/udsc/zestawienia-roczne",
                "measure_name": "Residence decisions",
                "measure_unit": "person",
                "source_granularity": (
                    "annual by citizenship, permit type, and outcome"
                ),
            },
            {
                "source_key": next_key + 2,
                "source_system": "UdSC",
                "source_dataset": "Valid residence documents",
                "source_url": "https://www.gov.pl/web/udsc/zestawienia-roczne",
                "measure_name": "Valid residence documents",
                "measure_unit": "person",
                "source_granularity": (
                    "annual snapshot by citizenship and document type"
                ),
            },
        ]
    )

    dimension = pd.concat([dimension, udsc_sources], ignore_index=True)
    return dimension.sort_values("source_key").reset_index(drop=True)


def build_dim_residence_type():
    """Creates the shared residence/document-type dimension."""

    dimension = pd.DataFrame(RESIDENCE_TYPES)
    dimension.insert(0, "residence_type_key", range(1, len(dimension) + 1))
    return dimension


def build_dim_decision_outcome():
    """Creates the controlled decision-outcome dimension."""

    dimension = pd.DataFrame(
        DECISION_OUTCOMES,
        columns=["decision_outcome_code", "decision_outcome_name_en"],
    )
    dimension.insert(0, "decision_outcome_key", range(1, len(dimension) + 1))
    dimension["is_final_positive"] = (
        dimension["decision_outcome_code"] == "positive"
    )
    dimension["is_final_negative"] = (
        dimension["decision_outcome_code"] == "negative"
    )
    return dimension


def build_lookup_maps(dim_citizenship, dim_residence_type, dim_outcome):
    """Creates stable mappings used by all three fact builders."""

    return {
        "citizenship": dim_citizenship.set_index("citizenship_name_pl")[
            "citizenship_key"
        ],
        "residence_type": dim_residence_type.set_index(
            "residence_type_code"
        )["residence_type_key"],
        "outcome": dim_outcome.set_index("decision_outcome_code")[
            "decision_outcome_key"
        ],
    }


def get_udsc_source_keys(dim_source):
    """Returns the three UdSC source surrogate keys."""

    source_map = dim_source.set_index("source_dataset")["source_key"]
    return {
        "applications": int(source_map["Annual residence applications"]),
        "decisions": int(source_map["Annual residence decisions"]),
        "documents": int(source_map["Valid residence documents"]),
    }


def build_fact_applications(silver, lookup_maps, source_key):
    """Builds the annual residence-application fact."""

    fact = silver.copy()
    fact["period_key"] = fact["reference_year"].astype(int)
    fact["citizenship_key"] = fact["citizenship_name_pl"].map(
        lookup_maps["citizenship"]
    )
    fact["residence_type_key"] = fact["permit_type"].map(
        lookup_maps["residence_type"]
    )
    fact["source_key"] = source_key
    output_columns = [
        "period_key",
        "citizenship_key",
        "residence_type_key",
        "source_key",
        "application_count",
        "source_file",
        "source_sheet",
        "pipeline_processed_at_utc",
    ]
    fact = fact[output_columns].sort_values(
        ["period_key", "citizenship_key", "residence_type_key"]
    ).reset_index(drop=True)
    fact.insert(0, "residence_application_key", range(1, len(fact) + 1))
    return fact


def build_fact_decisions(silver, lookup_maps, source_key):
    """Builds the annual residence-decision fact."""

    fact = silver.copy()
    fact["period_key"] = fact["reference_year"].astype(int)
    fact["citizenship_key"] = fact["citizenship_name_pl"].map(
        lookup_maps["citizenship"]
    )
    fact["residence_type_key"] = fact["permit_type"].map(
        lookup_maps["residence_type"]
    )
    fact["decision_outcome_key"] = fact["decision_outcome"].map(
        lookup_maps["outcome"]
    )
    fact["source_key"] = source_key
    output_columns = [
        "period_key",
        "citizenship_key",
        "residence_type_key",
        "decision_outcome_key",
        "source_key",
        "decision_count",
        "source_file",
        "source_sheet",
        "pipeline_processed_at_utc",
    ]
    fact = fact[output_columns].sort_values(
        [
            "period_key",
            "citizenship_key",
            "residence_type_key",
            "decision_outcome_key",
        ]
    ).reset_index(drop=True)
    fact.insert(0, "residence_decision_key", range(1, len(fact) + 1))
    return fact


def build_fact_valid_documents(silver, lookup_maps, source_key):
    """Builds the valid-document snapshot fact."""

    fact = silver.copy()
    fact["report_period_key"] = fact["reference_year"].astype(int)
    fact["snapshot_date"] = pd.to_datetime(
        fact["snapshot_date"], errors="raise"
    )
    fact["snapshot_period_key"] = fact["snapshot_date"].dt.year.astype(int)
    fact["citizenship_key"] = fact["citizenship_name_pl"].map(
        lookup_maps["citizenship"]
    )
    fact["residence_type_key"] = fact["document_type"].map(
        lookup_maps["residence_type"]
    )
    fact["source_key"] = source_key
    output_columns = [
        "report_period_key",
        "snapshot_period_key",
        "snapshot_date",
        "citizenship_key",
        "residence_type_key",
        "source_key",
        "valid_document_count",
        "source_file",
        "source_sheet",
        "pipeline_processed_at_utc",
    ]
    fact = fact[output_columns].sort_values(
        ["snapshot_date", "citizenship_key", "residence_type_key"]
    ).reset_index(drop=True)
    fact.insert(0, "valid_document_key", range(1, len(fact) + 1))
    return fact


def validate_gold_model(tables, silver_inputs):
    """Validates counts, keys, relationships, grains, and official totals."""

    dim_country = tables["dim_country_conformed"]
    dim_citizenship = tables["dim_citizenship"]
    dim_period = tables["dim_period_conformed"]
    dim_source = tables["dim_source_conformed"]
    dim_residence = tables["dim_residence_type"]
    dim_outcome = tables["dim_decision_outcome"]
    fact_applications = tables["fact_residence_applications"]
    fact_decisions = tables["fact_residence_decisions"]
    fact_documents = tables["fact_valid_documents"]
    errors = []

    expected_rows = {
        "fact_residence_applications": EXPECTED_APPLICATION_ROWS,
        "fact_residence_decisions": EXPECTED_DECISION_ROWS,
        "fact_valid_documents": EXPECTED_DOCUMENT_ROWS,
    }
    for table_name, expected in expected_rows.items():
        if len(tables[table_name]) != expected:
            errors.append(
                f"{table_name}: expected {expected}, "
                f"found {len(tables[table_name])}."
            )

    unique_key_checks = {
        "dim_country_conformed": "country_key",
        "dim_citizenship": "citizenship_key",
        "dim_period_conformed": "period_key",
        "dim_source_conformed": "source_key",
        "dim_residence_type": "residence_type_key",
        "dim_decision_outcome": "decision_outcome_key",
        "fact_residence_applications": "residence_application_key",
        "fact_residence_decisions": "residence_decision_key",
        "fact_valid_documents": "valid_document_key",
    }
    for table_name, key_column in unique_key_checks.items():
        table = tables[table_name]
        if table[key_column].isna().any() or table[key_column].duplicated().any():
            errors.append(f"{table_name}: invalid key {key_column}.")

    valid_keys = {
        "citizenship": set(dim_citizenship["citizenship_key"]),
        "period": set(dim_period["period_key"]),
        "source": set(dim_source["source_key"]),
        "residence": set(dim_residence["residence_type_key"]),
        "outcome": set(dim_outcome["decision_outcome_key"]),
    }
    foreign_key_checks = [
        (fact_applications, "citizenship_key", "citizenship"),
        (fact_applications, "period_key", "period"),
        (fact_applications, "source_key", "source"),
        (fact_applications, "residence_type_key", "residence"),
        (fact_decisions, "citizenship_key", "citizenship"),
        (fact_decisions, "period_key", "period"),
        (fact_decisions, "source_key", "source"),
        (fact_decisions, "residence_type_key", "residence"),
        (fact_decisions, "decision_outcome_key", "outcome"),
        (fact_documents, "citizenship_key", "citizenship"),
        (fact_documents, "report_period_key", "period"),
        (fact_documents, "snapshot_period_key", "period"),
        (fact_documents, "source_key", "source"),
        (fact_documents, "residence_type_key", "residence"),
    ]
    for fact, column, dimension_name in foreign_key_checks:
        if fact[column].isna().any():
            errors.append(f"Null foreign key: {column}.")
        elif not set(fact[column]).issubset(valid_keys[dimension_name]):
            errors.append(f"Broken foreign key: {column}.")

    application_totals = (
        silver_inputs["applications"]
        .groupby("reference_year")["application_count"]
        .sum()
        .to_dict()
    )
    if application_totals != EXPECTED_APPLICATION_TOTALS:
        errors.append("Application totals do not match official values.")

    negative_totals = (
        silver_inputs["decisions"].loc[
            silver_inputs["decisions"]["decision_outcome"] == "negative"
        ]
        .groupby("reference_year")["decision_count"]
        .sum()
        .to_dict()
    )
    if negative_totals != EXPECTED_NEGATIVE_DECISIONS:
        errors.append("Negative-decision totals do not match official values.")

    core_types = {
        "temporary_residence",
        "permanent_residence",
        "long_term_eu_residence",
    }
    stock_totals = (
        silver_inputs["documents"].loc[
            silver_inputs["documents"]["document_type"].isin(core_types)
        ]
        .groupby("reference_year")["valid_document_count"]
        .sum()
        .to_dict()
    )
    if stock_totals != EXPECTED_CORE_DOCUMENT_STOCK:
        errors.append("Document-stock totals do not match official values.")

    if dim_country["iso3_code"].duplicated().any():
        errors.append("Conformed country ISO3 values are not unique.")

    if errors:
        raise ValueError("Gold validation failed:\n- " + "\n- ".join(errors))

    print("Gold validation passed:")
    for table_name, table in tables.items():
        print(f"  {table_name}: {len(table):,} rows")
    print("  Existing GUS country keys were preserved")
    print("  All surrogate and foreign keys are valid")
    print("  Official applications, decisions, and stock totals match")


def ensure_directory(file_system, directory_path):
    """Creates every Azure Data Lake directory level if absent."""

    current_path = ""
    for part in directory_path.split("/"):
        current_path = f"{current_path}/{part}" if current_path else part
        try:
            file_system.get_directory_client(
                current_path
            ).create_directory()
        except ResourceExistsError:
            pass


def save_and_upload_table(
    file_system,
    table_name,
    dataframe,
    local_directory,
    azure_directory,
):
    """Writes one Gold table locally and uploads it as Parquet."""

    local_directory.mkdir(parents=True, exist_ok=True)
    ensure_directory(file_system, azure_directory)
    local_path = local_directory / f"{table_name}.parquet"
    dataframe.to_parquet(local_path, index=False, engine="pyarrow")
    remote_path = f"{azure_directory}/{local_path.name}"
    file_system.get_file_client(remote_path).upload_data(
        local_path.read_bytes(), overwrite=True
    )
    print(f"Uploaded to {CURATED_CONTAINER}/{remote_path}")
    return f"{CURATED_CONTAINER}/{remote_path}"


def save_and_upload_tables(file_system, tables):
    """Writes conformed dimensions, UdSC dimensions, and UdSC facts."""

    conformed_names = {
        "dim_country_conformed",
        "dim_period_conformed",
        "dim_source_conformed",
    }
    udsc_dimension_names = {
        "dim_citizenship",
        "dim_residence_type",
        "dim_decision_outcome",
    }
    output_paths = {}

    for table_name, dataframe in tables.items():
        if table_name in conformed_names:
            local_directory = LOCAL_CONFORMED_DIRECTORY
            azure_directory = CONFORMED_DIMENSIONS_DIRECTORY
        elif table_name in udsc_dimension_names:
            local_directory = LOCAL_UDSC_DIMENSIONS_DIRECTORY
            azure_directory = UDSC_DIMENSIONS_DIRECTORY
        else:
            local_directory = LOCAL_UDSC_FACTS_DIRECTORY
            azure_directory = UDSC_FACTS_DIRECTORY

        output_paths[table_name] = save_and_upload_table(
            file_system,
            table_name,
            dataframe,
            local_directory,
            azure_directory,
        )

    return output_paths


def upload_manifest(file_system, tables, output_paths):
    """Writes model grain, lineage, validation, and output metadata."""

    manifest = {
        "processedAtUtc": datetime.now(timezone.utc).isoformat(),
        "modelType": "fact constellation with snowflaked citizenship mapping",
        "validationStatus": "passed",
        "sourcePaths": {
            "applications": f"{CURATED_CONTAINER}/{SILVER_APPLICATIONS_PATH}",
            "decisions": f"{CURATED_CONTAINER}/{SILVER_DECISIONS_PATH}",
            "validDocuments": f"{CURATED_CONTAINER}/{SILVER_DOCUMENTS_PATH}",
            "gusCountryDimension": f"{CURATED_CONTAINER}/{GUS_DIM_COUNTRY_PATH}",
        },
        "tables": {
            name: {
                "rowCount": len(dataframe),
                "outputPath": output_paths[name],
            }
            for name, dataframe in tables.items()
        },
        "grain": {
            "fact_residence_applications": (
                "one row per year, UdSC citizenship label, and permit type"
            ),
            "fact_residence_decisions": (
                "one row per year, UdSC citizenship label, permit type, "
                "and decision outcome"
            ),
            "fact_valid_documents": (
                "one row per report year, snapshot date, UdSC citizenship "
                "label, and document type"
            ),
        },
        "dataQuality": {
            "officialTotalColumn": "Razem/Suma",
            "sexBreakdownNotRecalculated": True,
            "translatorOutputPreservedInSilver": True,
            "controlledCountryNamesAppliedInGold": True,
            "temporaryProtectionSeparatedFromOrdinaryResidence": True,
        },
    }
    manifest_bytes = json.dumps(
        manifest, ensure_ascii=False, indent=2
    ).encode("utf-8")
    ensure_directory(file_system, "gold/udsc")
    file_system.get_file_client(UDSC_MANIFEST_PATH).upload_data(
        manifest_bytes, overwrite=True
    )
    print(f"Uploaded to {CURATED_CONTAINER}/{UDSC_MANIFEST_PATH}")


def main():
    """Executes the complete UdSC Silver-to-Gold transformation."""

    print(f"UdSC Gold transformer version: {SCRIPT_VERSION}")
    file_system = get_curated_file_system()
    inputs = load_inputs(file_system)
    labels = collect_citizenship_labels(inputs)
    country_reference = build_country_reference(labels)

    print("Building conformed dimensions...")
    dim_country = build_conformed_dim_country(
        inputs["gus_dim_country"], country_reference
    )
    dim_period = build_conformed_dim_period(inputs["gus_dim_period"])
    dim_source = build_conformed_dim_source(inputs["gus_dim_source"])

    print("Building UdSC dimensions...")
    dim_citizenship = build_dim_citizenship(
        country_reference, dim_country
    )
    dim_residence_type = build_dim_residence_type()
    dim_decision_outcome = build_dim_decision_outcome()

    lookup_maps = build_lookup_maps(
        dim_citizenship,
        dim_residence_type,
        dim_decision_outcome,
    )
    source_keys = get_udsc_source_keys(dim_source)

    print("Building UdSC facts...")
    fact_applications = build_fact_applications(
        inputs["applications"], lookup_maps, source_keys["applications"]
    )
    fact_decisions = build_fact_decisions(
        inputs["decisions"], lookup_maps, source_keys["decisions"]
    )
    fact_documents = build_fact_valid_documents(
        inputs["documents"], lookup_maps, source_keys["documents"]
    )

    tables = {
        "dim_country_conformed": dim_country,
        "dim_period_conformed": dim_period,
        "dim_source_conformed": dim_source,
        "dim_citizenship": dim_citizenship,
        "dim_residence_type": dim_residence_type,
        "dim_decision_outcome": dim_decision_outcome,
        "fact_residence_applications": fact_applications,
        "fact_residence_decisions": fact_decisions,
        "fact_valid_documents": fact_documents,
    }

    validate_gold_model(tables, inputs)
    output_paths = save_and_upload_tables(file_system, tables)
    upload_manifest(file_system, tables, output_paths)

    print("Done. UdSC Silver-to-Gold transformation completed successfully.")


if __name__ == "__main__":
    main()