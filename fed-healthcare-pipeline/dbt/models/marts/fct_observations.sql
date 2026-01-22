-- Marts Model: Multi-source observation facts
-- Gold layer: Analytics-ready clinical observations from FHIR, HL7, and OCR sources

WITH fhir_obs AS (
    SELECT
        observation_id AS obs_key,
        REPLACE(patient_ref, 'Patient/', '') AS patient_key,
        loinc_code,
        observation_name AS obs_name,
        CAST(value AS DOUBLE) AS value_numeric,
        unit,
        observation_time AS obs_datetime,
        source_system,
        ingested_at
    FROM read_parquet('../data/processed/bronze/observations_fhir/**/*.parquet')
),

hl7_obs AS (
    SELECT
        NULL AS obs_key,
        NULL AS patient_key,
        loinc_code,
        observation_name AS obs_name,
        TRY_CAST(value AS DOUBLE) AS value_numeric,
        unit,
        CAST(NULL AS TIMESTAMP) AS obs_datetime,
        source_system,
        ingested_at
    FROM read_parquet('../data/processed/bronze/observations_hl7/**/*.parquet')
),

ocr_labs AS (
    SELECT
        NULL AS obs_key,
        mrn AS patient_key,
        NULL AS loinc_code,
        test_name AS obs_name,
        CAST(value AS DOUBLE) AS value_numeric,
        unit,
        CAST(NULL AS TIMESTAMP) AS obs_datetime,
        source_system,
        ingested_at
    FROM read_parquet('../data/processed/bronze/labs_ocr/**/*.parquet')
),

combined AS (
    SELECT * FROM fhir_obs
    UNION ALL
    SELECT * FROM hl7_obs
    UNION ALL
    SELECT * FROM ocr_labs
),

with_row_number AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY ingested_at, obs_name) AS row_num
    FROM combined
)

SELECT
    COALESCE(
        obs_key || '-' || CAST(ROW_NUMBER() OVER (PARTITION BY obs_key ORDER BY ingested_at) AS VARCHAR),
        MD5(CONCAT(
            COALESCE(patient_key, 'UNKNOWN'),
            '-',
            COALESCE(obs_name, 'UNKNOWN'),
            '-',
            CAST(value_numeric AS VARCHAR),
            '-',
            CAST(row_num AS VARCHAR)
        ))
    ) AS observation_key,
    patient_key,
    loinc_code,
    obs_name,
    value_numeric,
    unit,
    obs_datetime,
    source_system,
    ingested_at
FROM with_row_number
WHERE value_numeric IS NOT NULL
