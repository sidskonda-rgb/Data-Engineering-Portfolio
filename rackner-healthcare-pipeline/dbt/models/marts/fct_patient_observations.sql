-- Analytics: patient observation facts
WITH observations AS (
    SELECT * FROM read_parquet('../data/processed/bronze/observations/*.parquet')
),

patients AS (
    SELECT * FROM {{ ref('stg_patients') }}
)

SELECT
    obs.observation_id,
    REPLACE(obs.patient_ref, 'Patient/', '') AS patient_id,
    obs.loinc_code,
    obs.observation_name,
    obs.value,
    obs.unit,
    obs.observation_time,
    p.age AS patient_age,
    p.gender
FROM observations obs
LEFT JOIN patients p ON REPLACE(obs.patient_ref, 'Patient/', '') = p.patient_id