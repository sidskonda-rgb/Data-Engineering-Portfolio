# Rackner-Style Lakehouse Pipeline (AWS + Airflow + Spark + dbt + Iceberg)

This repo demonstrates an end-to-end “lakehouse” data engineering pipeline on AWS using the same core building blocks commonly used in federal data modernization work:
- **Airflow** for orchestration
- **Spark** for transformation
- **Apache Iceberg** tables on **S3** (schema evolution + table semantics)
- **AWS Glue Data Catalog** as the metastore
- **dbt** for modeling + tests on top of curated tables

The pipeline intentionally handles both:
1) nested, messy semi-structured clinical-style JSON (FHIR-like)
2) non-typical inputs (PDF/OCR) via **Amazon Textract** into queryable tables

## Architecture
**S3** layout:
- `raw/`         raw drops (FHIR JSON + PDFs)
- `textract/`    OCR extraction outputs (JSON)
- `warehouse/`   Iceberg table data files
- `scripts/`     Spark job scripts

**Glue Catalog** namespaces:
- `bronze` (close to source, but tabular)
- `silver` (typed, deduped, conformed; modeling begins here)
- `gold`   (analytics marts produced by dbt)

**Airflow DAG** (local Docker Airflow):
1. Detect new raw data in S3
2. Run Textract for PDFs and store results to `textract/`
3. Spark job: raw -> bronze Iceberg tables
4. Spark job: bronze -> silver Iceberg tables (grain + dedup rules)
5. dbt run/test: silver -> gold marts + data quality checks

## Data Model (MVP)
### Bronze (Iceberg)
- `bronze_fhir_patient`
- `bronze_fhir_encounter`
- `bronze_fhir_observation`
- `bronze_docs_text` (from Textract output; one row per document block)

### Silver (Iceberg)
- `silver_dim_patient` (grain: 1 row per patient_id)
- `silver_fct_encounter` (grain: 1 row per encounter_id; dedup = latest ingested_at)
- `silver_fct_observation` (grain: patient_id + obs_time + obs_code)
- `silver_fct_doc_blocks` (normalized text blocks with confidence + provenance)

### Gold (dbt)
- `gold_patient_encounter_summary`
- `gold_docs_quality`
- (optional) prevalence / utilization marts

## Why Iceberg
Iceberg provides table semantics on S3:
- schema evolution without breaking readers
- partition evolution
- snapshot-based reads (auditability / reproducibility)

## How to Run (High Level)
1. Upload raw data to:
   - `s3://<bucket>/raw/fhir/dt=YYYY-MM-DD/`
   - `s3://<bucket>/raw/docs/dt=YYYY-MM-DD/`

2. Run the Airflow DAG (`airflow/dags/lakehouse_dag.py`), which:
   - runs Textract for PDFs
   - triggers Spark jobs to write Iceberg tables in Glue Catalog
   - runs dbt models/tests

## Operational Notes
See `docs/runbook.md` for first checks (IAM/S3 perms, missing partitions, bad JSON, Textract job status, Glue job logs).
