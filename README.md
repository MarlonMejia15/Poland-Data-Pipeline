# Poland Data Pipeline 

I built this project to practice an end-to-end data workflow in Azure using
real public data from Poland. The pipeline downloads data from GUS and UdSC,
keeps the original files in Azure Data Lake, cleans and translates the data,
and prepares Parquet tables for a Power BI dashboard.

The dashboard will focus on foreigners in Poland: citizenship, residence
applications, decisions, negative decisions, and valid residence documents. The data
pipeline is complete through the Gold layer. Power BI is the next step.

## Data used in the project

### GUS Census 2021

The first source is the Local Data Bank (Bank Danych Lokalnych — BDL),
maintained by Statistics Poland (GUS). I used its REST API to obtain data from
the 2021 National Population and Housing Census.

The selected dataset is the usual-residence population by citizenship and sex
(subject P4306). It covers Poland and the 16 voivodeships.

  * BDL portal: https://bdl.stat.gov.pl/bdl/start
  * BDL API documentation: https://api.stat.gov.pl/Home/BdlApi?lang=en
  * API base URL: https://bdl.stat.gov.pl/api/v1

### UdSC annual reports

The second source is the Polish Office for Foreigners (UdSC). UdSC publishes
annual Excel reports about procedures involving foreigners. I downloaded the
complete reports for 2021 through 2025.

  * UdSC annual reports: https://www.gov.pl/web/udsc/zestawienia-roczne

From these workbooks, the pipeline extracts:

    residence applications;
    decisions and their outcomes;
    valid residence documents at the reporting date.

These measures are different from the GUS census population. For example, a
valid-document total is a stock at a specific date, while applications and
decisions are annual flows. I will keep them separate in Power BI instead of
combining them into one population figure.

## Pipeline

The workflow is:

 GUS API / UdSC Excel -> Raw -> Silver -> Gold -> Power BI

* Raw keeps the original JSON responses and Excel files. UdSC workbooks are
    stored by year and include a manifest with SHA-256 checksums.

* Silver contains cleaned CSV and Parquet files. This is where the Excel
    layouts are standardized, data types are assigned, Polish labels are
    translated, and source totals are validated.

* Gold contains dimensions and fact tables for Power BI. The Gold folders are
    separated into gus, udsc, and conformed. The conformed dimensions provide
    shared country, period, and source keys without mixing the meaning of the two
    datasets.

* Azure AI Translator is used only for unique Polish citizenship labels. The
    translations are cached locally, so running the pipeline again does not send
    the same labels to the API and consume the free quota unnecessarily.

## Data model

The model is a fact constellation with shared dimensions. UdSC also has a
separate citizenship dimension because different source labels can refer to
the same country. This lets me keep the original label for traceability while
still connecting it to a standardized country and ISO code.

### GUS tables

 * `dim_country`: 187 rows
 * `dim_geography`: 17 rows
 * `dim_period`: 1 row
 * `dim_source`: 1 row
 * `fact_citizenship_population`: 3,179 rows
 * `fact_citizenship_summary`: 51 rows

The main census fact contains only real countries. Aggregate, stateless, and
unknown rows are stored in the summary fact so they cannot be accidentally
added to the country totals.

### Shared and UdSC tables

* `dim_country_conformed`: 206 rows
* `dim_period_conformed`: 6 rows
* `dim_source_conformed`: 4 rows
* `dim_citizenship`: 206 rows
* `dim_residence_type`: 17 rows
* `dim_decision_outcome`: 4 rows
* `fact_residence_applications`: 1,682 rows
* `fact_residence_decisions`: 6,616 rows
* `fact_valid_documents`: 15,226 rows

The existing GUS country keys are preserved when UdSC countries and
territories are added to the conformed dimension.

## Problems found in the source files

The UdSC reports look similar from year to year, but their structure is not
consistent enough to concatenate them directly.

   * The residence columns appear in a different order in 2021, 2022, and 2023-2025.
   * The 2021 workbook has an additional title row before the real header.
   * Some headers have trailing spaces.
   * Some sheets use merged cells and contain duplicated captions such as
     POZYTYWNA Suma where the second caption should refer to negative decisions.
   * In the 2022 temporary-residence applications sheet, the official total was
     labeled as a second SYRIA row.
   * The 2022 sex breakdown contains small one-person differences from the
     published total in some records.

To avoid silent errors, the Silver transformation searches for the real header
row, normalizes the labels, and maps columns by name instead of position. For
decision sheets, it locates the start of each outcome group and uses the
validated relative position of the official total.

The pipeline uses the official Razem/Suma column instead of recalculating the
total as K + M. This decision was made after finding one-person differences
in the 2022 sex breakdown. Using the official total makes the dashboard agree
with the figures published by UdSC.

Silver keeps the Azure Translator result unchanged. Corrections such as
standard country spelling, ISO codes, and duplicate country aliases are applied
in Gold, where they are easier to control and audit.

Before uploading Gold, the scripts validate row counts, unique keys, foreign
keys, model grain, and official yearly totals. If one of these checks fails, the
upload stops.

## Totals used for validation

* **2021:** 392,715 applications, 40,342 negative decisions, and 458,272 core valid documents.
* **2022:** 536,064 applications, 36,293 negative decisions, and 639,592 core valid documents.
* **2023:** 608,900 applications, 28,043 negative decisions, and 831,639 core valid documents.
* **2024:** 509,783 applications, 34,691 negative decisions, and 891,671 core valid documents.
* **2025:** 562,801 applications, 31,812 negative decisions, and 955,176 core valid documents.

* Core valid documents combine temporary residence, permanent residence, and EU long-term resident documents.


## Project folders

 * `ingestion/` contains the GUS discovery/extraction scripts and the UdSC annual
    workbook ingestion.
 * `transformation/silver/` contains the Raw-to-Silver transformations.
 * `transformation/gold/` contains the Silver-to-Gold transformations.
 * `data/raw/`, `data/silver/`, and `data/gold/` store local copies of the pipeline data layers.
 * `data/cache/` stores reusable translation results.
 *  The `data` folder, virtual environment, and .env file are ignored by Git.

## Running the project

Create and activate the environment in PowerShell:

    python -m venv venv
    venv\Scripts\Activate.ps1
    pip install -r requirements.txt

The local .env file uses the following variables:

    AZURE_STORAGE_ACCOUNT_NAME = <storage-account-name>
    AZURE_STORAGE_ACCOUNT_KEY = <storage-account-key>
    TRANSLATOR_KEY = <translator-key>
    TRANSLATOR_ENDPOINT = https://api.cognitive.microsofttranslator.com
    AZURE_TRANSLATOR_REGION = <translator-resource-region>

Run the commands from the repository root.

### Ingestion

    python .\ingestion\ingest_gus_all_citizenships.py
    python .\ingestion\ingest_udsc_annual.py

The other GUS ingestion scripts show the discovery steps used to find the final
census subject and citizenship variables.

### Silver

    python .\transformation\silver\transform_gus_citizenship_silver.py
    python .\transformation\silver\transform_udsc_residence_silver.py

### Gold

    python .\transformation\gold\transform_gus_citizenship_gold.py
    python .\transformation\gold\transform_udsc_residence_gold.py

GUS Gold runs first because the UdSC model extends its country, period, and
source dimensions.


## Notes for the dashboard

There are a few comparisons I will avoid in Power BI:

  * Applications and decisions from the same year are not necessarily the same
    cases. Their difference is not automatically a backlog, and it should not be
    presented as a cohort approval rate.
  * Valid documents are not the same as new permits issued during the year.
  * Citizenship is not the same as country of birth or ethnicity.
  * The GUS census is from 2021, while the UdSC series covers 2021-2025.
  * Partial 2026 data is not included in the yearly comparison.

The planned report will have a general overview, residence procedures,
citizenship rankings, a separate GUS Census 2021 page, and one focused page for
Latin America and the Caribbean.

## Tools

    Python, Pandas, Requests, PyArrow, OpenPyXL, country_converter, Azure Data
    Lake Storage Gen2, Azure AI Translator, GitHub, and Power BI.