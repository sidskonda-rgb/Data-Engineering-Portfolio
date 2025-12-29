-- Staging layer: clean patient records
WITH source AS (
    SELECT * FROM read_parquet('../data/processed/bronze/patients/*.parquet')
)

SELECT
    patient_id,
    family_name,
    given_name,
    gender,
    birth_date,
    YEAR(CURRENT_DATE) - YEAR(birth_date) AS age,
    ingested_at
FROM source