# Health Lakehouse Pipeline (AWS Glue + S3 + Athena)

This project builds an end-to-end, AWS-native data pipeline using production-level healthcare-style data, modeled analytics tables that are safe to rerun.

The goal is to demonstrate:
- raw data landing in S3
- repeatable ETL with Glue PySpark
- curated data modeling (facts/dims)
- queryable gold outputs in Athena
- basic data quality reporting

## Dataset
I use **Synthea** synthetic EHR exports in **FHIR JSON** format. The data is intentionally nested and inconsistent (optional fields, arrays, varying resource structures), which makes it a good stand-in for real-world clinical pipelines without PHI risk.

## Architecture
**S3** stores everything in a simple lake layout:
- `raw/`     - immutable drops (FHIR bundles)
- `bronze/`  - lightly structured tables (still close to source)
- `silver/`  - conformed entities + deduped facts (model-ready)
- `gold/`    - analytics outputs + data quality dashboard

**Glue** runs PySpark ETL jobs:
1. `raw_to_bronze_synthea` extracts FHIR resources into bronze Parquet tables
2. `bronze_to_silver_conform` models patient + encounter + observation into silver tables

**Athena** queries silver and materializes gold datasets using SQL.

## Data Model (MVP)
### Silver
- `silver_dim_patient`
  - Grain: 1 row per `patient_id` (SCD1 for MVP)
- `silver_fct_encounter`
  - Grain: 1 row per `encounter_id`
  - Dedup: deterministic “latest wins” rule
- `silver_fct_observation`
  - Grain: `patient_id` + `obs_datetime` + `obs_code` (MVP)

### Gold
- `gold_population_summary`
  - example population metrics by time bucket / category
- `gold_data_quality_dashboard`
  - row counts, null rates on keys, duplicates removed

## Design Decisions (why I did it this way)
- I partition by `dt=YYYY-MM-DD` to keep reruns and backfills predictable.
- Bronze stays “close to raw” so I can reprocess without losing evidence.
- Silver is where I enforce grain + dedup rules so downstream queries don’t lie.
- I kept the MVP to 3 core FHIR resources (Patient/Encounter/Observation) so the pipeline is complete end-to-end before expanding scope.

## How to Run (High Level)
1) Upload FHIR JSON bundles to:
`s3://<bucket>/raw/synthea_fhir/dt=<YYYY-MM-DD>/`

2) Run Glue job:
- `raw_to_bronze_synthea`
Outputs:
- `s3://<bucket>/bronze/patient/dt=<...>/`
- `s3://<bucket>/bronze/encounter/dt=<...>/`
- `s3://<bucket>/bronze/observation/dt=<...>/`

3) Run Glue job:
- `bronze_to_silver_conform`
Outputs:
- `s3://<bucket>/silver/...`

4) Run Athena SQL in `src/sql/` to generate gold outputs.

## Operational Notes / Debugging
See `docs/runbook.md` for the first checks I do when a job fails (permissions, schema drift, missing partitions, and bad JSON records).

## Future Enhancements
- Add **Apache Iceberg** tables registered in Glue Catalog for schema evolution + time travel.
- Add NPPES (NPI registry) as a second source for provider dimension modeling.
- Orchestrate Glue + Athena runs with Airflow (MWAA or local Docker Airflow).
