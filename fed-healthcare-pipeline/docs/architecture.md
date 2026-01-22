# Pipeline Architecture

## Data Flow
1. **Raw Layer**: FHIR JSON bundles land in `data/raw/`
2. **Bronze Layer**: Spark extracts resources → Parquet (schema-on-write)
3. **Silver/Gold**: dbt transforms into analytics-ready tables

## FHIR Mapping
- `Patient` → `stg_patients` (demographics)
- `Observation` → linked to patients via `subject.reference`
- LOINC codes preserved for clinical context

## Governance
- `ingested_at` timestamps for audit trail
- Parquet provides schema evolution capability
- dbt tests enforce data contracts