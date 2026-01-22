-- Staging Model: Combined patient demographics from FHIR and HL7 sources
-- Silver layer: Cleaned and enriched patient data

WITH fhir_patients AS (
    SELECT
        patient_id,
        mrn,
        family_name,
        given_name,
        gender,
        birth_date,
        city,
        state,
        source_system,
        ingested_at
    FROM read_parquet('../data/processed/bronze/patients_fhir/**/*.parquet')
),

hl7_patients AS (
    SELECT
        NULL AS patient_id,
        mrn,
        family_name,
        given_name,
        gender,
        CAST(NULL AS TIMESTAMP) AS birth_date,
        NULL AS city,
        NULL AS state,
        source_system,
        ingested_at
    FROM read_parquet('../data/processed/bronze/patients_hl7/**/*.parquet')
),

combined AS (
    SELECT * FROM fhir_patients
    UNION ALL
    SELECT * FROM hl7_patients
)

SELECT
    COALESCE(patient_id, 'HL7-' || mrn) AS patient_key,
    mrn,
    family_name,
    given_name,
    gender,
    birth_date,
    CASE
        WHEN birth_date IS NOT NULL
        THEN YEAR(CURRENT_DATE) - YEAR(birth_date)
        ELSE NULL
    END AS age,
    city,
    state,
    source_system,
    ingested_at,
    ROW_NUMBER() OVER (PARTITION BY mrn ORDER BY ingested_at DESC) AS recency_rank
FROM combined
WHERE mrn IS NOT NULL OR patient_id IS NOT NULL
