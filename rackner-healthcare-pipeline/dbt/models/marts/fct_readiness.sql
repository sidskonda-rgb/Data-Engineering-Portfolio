-- Marts Model: Military deployment readiness
-- Gold layer: Soldier medical readiness status with business logic

SELECT
    soldier_id,
    unit,
    deployment_status,
    medical_clearance,
    dental_class,
    last_pha_date,
    CASE
        WHEN last_pha_date IS NOT NULL
        THEN DATE_DIFF('day', last_pha_date, CURRENT_DATE)
        ELSE NULL
    END AS days_since_pha,
    CASE
        WHEN last_pha_date IS NOT NULL AND DATE_DIFF('day', last_pha_date, CURRENT_DATE) > 365
        THEN 'PHA_OVERDUE'
        WHEN medical_clearance = FALSE
        THEN 'MEDICAL_HOLD'
        WHEN dental_class >= 3
        THEN 'DENTAL_HOLD'
        ELSE 'CLEARED'
    END AS readiness_flag,
    bmi,
    CASE
        WHEN bmi < 18.5 THEN 'UNDERWEIGHT'
        WHEN bmi >= 18.5 AND bmi < 25 THEN 'NORMAL'
        WHEN bmi >= 25 AND bmi < 30 THEN 'OVERWEIGHT'
        ELSE 'OBESE'
    END AS bmi_category,
    deployable,
    notes,
    source_system,
    ingested_at
FROM read_parquet('../data/processed/bronze/readiness_csv/**/*.parquet')