from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, current_timestamp, 
    regexp_extract, when, trim, lit, coalesce
)
from pyspark.sql.types import IntegerType, DoubleType

def main():
    spark = SparkSession.builder \
        .appName("Multi-Format-Healthcare-ETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("=" * 80)
    print("PROCESSING MULTI-FORMAT HEALTHCARE DATA")
    print("=" * 80)
    
    # ===========================
    # 1. FHIR JSON (Semi-Structured)
    # ===========================
    print("\n[1/4] Processing FHIR JSON...")
    df_fhir_raw = spark.read.option("multiline", "true").json("../../data/raw/fhir/*.json")
    
    # Extract Patients
    df_patients = df_fhir_raw.select(
        explode("entry").alias("entry")
    ).filter(
        col("entry.resource.resourceType") == "Patient"
    ).select(
        col("entry.resource.id").alias("patient_id"),
        col("entry.resource.identifier")[0]["value"].alias("mrn"),
        col("entry.resource.name")[0]["family"].alias("family_name"),
        col("entry.resource.name")[0]["given"][0].alias("given_name"),
        col("entry.resource.gender").alias("gender"),
        to_timestamp(col("entry.resource.birthDate")).alias("birth_date"),
        col("entry.resource.address")[0]["city"].alias("city"),
        col("entry.resource.address")[0]["state"].alias("state"),
        lit("FHIR").alias("source_system"),
        current_timestamp().alias("ingested_at")
    )
    
    # Extract Observations
    df_observations = df_fhir_raw.select(
        explode("entry").alias("entry")
    ).filter(
        col("entry.resource.resourceType") == "Observation"
    ).select(
        col("entry.resource.id").alias("observation_id"),
        col("entry.resource.subject.reference").alias("patient_ref"),
        col("entry.resource.code.coding")[0]["code"].alias("loinc_code"),
        col("entry.resource.code.coding")[0]["display"].alias("observation_name"),
        col("entry.resource.valueQuantity.value").alias("value"),
        col("entry.resource.valueQuantity.unit").alias("unit"),
        to_timestamp(col("entry.resource.effectiveDateTime")).alias("observation_time"),
        col("entry.resource.status").alias("status"),
        lit("FHIR").alias("source_system"),
        current_timestamp().alias("ingested_at")
    )
    
    print(f"  ✓ Extracted {df_patients.count()} patients, {df_observations.count()} observations")
    
    # ===========================
    # 2. HL7 v2 Messages (Pipe-Delimited Legacy)
    # ===========================
    print("\n[2/4] Processing HL7 v2 Messages...")
    df_hl7_raw = spark.read.text("../../data/raw/hl7/*.hl7")
    
    # Parse PID segment (Patient Demographics)
    df_hl7_patients = df_hl7_raw.filter(
        col("value").startswith("PID")
    ).select(
        regexp_extract(col("value"), r'PID\|[^|]*\|\|([^|^]+)', 1).alias("mrn"),
        regexp_extract(col("value"), r'\|\|([^^]+)\^([^^]+)\^([^^]*)\|', 1).alias("family_name"),
        regexp_extract(col("value"), r'\|\|([^^]+)\^([^^]+)\^([^^]*)\|', 2).alias("given_name"),
        regexp_extract(col("value"), r'\|\|[^|]+\|([^|]*)\|', 1).alias("dob_raw"),
        regexp_extract(col("value"), r'\|\|[^|]+\|([^|]*)\|([^|]*)\|', 2).alias("gender"),
        lit("HL7v2").alias("source_system"),
        current_timestamp().alias("ingested_at")
    ).filter(
        col("mrn") != ""
    )
    
    # Parse OBX segments (Observations/Vitals)
    df_hl7_obs = df_hl7_raw.filter(
        col("value").startswith("OBX")
    ).select(
        regexp_extract(col("value"), r'OBX\|[^|]*\|[^|]*\|([^|^]+)', 1).alias("loinc_code"),
        regexp_extract(col("value"), r'\|([^^]+)\^([^^]+)\^', 2).alias("observation_name"),
        regexp_extract(col("value"), r'\|\|([^|]+)\|([^|]*)\|', 1).alias("value"),
        regexp_extract(col("value"), r'\|\|([^|]+)\|([^|]*)\|', 2).alias("unit"),
        regexp_extract(col("value"), r'\|([^|]*)\|\|\|([^|]*)\|', 2).alias("status"),
        lit("HL7v2").alias("source_system"),
        current_timestamp().alias("ingested_at")
    ).filter(
        col("value") != ""
    )
    
    print(f"  ✓ Parsed {df_hl7_patients.count()} HL7 patients, {df_hl7_obs.count()} HL7 observations")
    
    # ===========================
    # 3. PDF/OCR Text (Unstructured → Semi-Structured)
    # ===========================
    print("\n[3/4] Processing OCR/PDF Text...")
    df_pdf_raw = spark.read.text("../../data/raw/pdfs/*.txt")
    
    # Extract lab values from OCR text
    df_pdf_labs = df_pdf_raw.select(
        regexp_extract(col("value"), r'MRN:\s*(\d+)', 1).alias("mrn"),
        regexp_extract(col("value"), r'Patient:\s*([A-Z]+),\s*([A-Z]+)', 1).alias("family_name"),
        regexp_extract(col("value"), r'(Glucose|Creatinine|Sodium|Potassium)\s+(\d+\.?\d*)\s+([a-zA-Z/]+)', 1).alias("test_name"),
        regexp_extract(col("value"), r'(Glucose|Creatinine|Sodium|Potassium)\s+(\d+\.?\d*)\s+([a-zA-Z/]+)', 2).alias("value"),
        regexp_extract(col("value"), r'(Glucose|Creatinine|Sodium|Potassium)\s+(\d+\.?\d*)\s+([a-zA-Z/]+)', 3).alias("unit"),
        lit("PDF_OCR").alias("source_system"),
        current_timestamp().alias("ingested_at")
    ).filter(
        col("test_name") != ""
    )
    
    print(f"  ✓ Extracted {df_pdf_labs.count()} lab values from OCR text")
    
    # ===========================
    # 4. Messy CSV (Structured but Dirty)
    # ===========================
    print("\n[4/4] Processing Dirty CSV...")
    df_csv_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("../../data/raw/csv/*.csv")
    
    # Clean and standardize
    df_readiness = df_csv_raw.select(
        trim(col("soldier_id")).alias("soldier_id"),
        coalesce(trim(col("unit")), lit("UNASSIGNED")).alias("unit"),
        trim(col("deployment_status")).alias("deployment_status"),
        when(col("medical_clearance") == "YES", True)
            .when(col("medical_clearance") == "NO", False)
            .otherwise(None).alias("medical_clearance"),
        col("dental_class").cast(IntegerType()).alias("dental_class"),
        to_timestamp(col("last_pha_date"), "yyyy-MM-dd").alias("last_pha_date"),
        col("bmi").cast(DoubleType()).alias("bmi"),
        col("deployable").cast("boolean").alias("deployable"),
        coalesce(trim(col("notes")), lit("")).alias("notes"),
        lit("CSV").alias("source_system"),
        current_timestamp().alias("ingested_at")
    )
    
    print(f"  ✓ Cleaned {df_readiness.count()} readiness records")
    
    # ===========================
    # WRITE TO BRONZE LAYER
    # ===========================
    print("\n" + "=" * 80)
    print("WRITING TO BRONZE LAYER (Parquet)")
    print("=" * 80)
    
    df_patients.write.mode("overwrite").parquet("../../data/processed/bronze/patients_fhir")
    df_observations.write.mode("overwrite").parquet("../../data/processed/bronze/observations_fhir")
    df_hl7_patients.write.mode("overwrite").parquet("../../data/processed/bronze/patients_hl7")
    df_hl7_obs.write.mode("overwrite").parquet("../../data/processed/bronze/observations_hl7")
    df_pdf_labs.write.mode("overwrite").parquet("../../data/processed/bronze/labs_ocr")
    df_readiness.write.mode("overwrite").parquet("../../data/processed/bronze/readiness_csv")
    
    print("\n✅ ETL COMPLETE - All formats processed")
    print(f"   - FHIR JSON: {df_patients.count()} patients, {df_observations.count()} obs")
    print(f"   - HL7 v2: {df_hl7_patients.count()} patients, {df_hl7_obs.count()} obs")
    print(f"   - PDF/OCR: {df_pdf_labs.count()} lab results")
    print(f"   - CSV: {df_readiness.count()} readiness records")
    
    spark.stop()

if __name__ == "__main__":
    main()