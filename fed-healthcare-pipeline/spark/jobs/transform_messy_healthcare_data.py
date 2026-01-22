"""
Multi-Format Healthcare Data ETL Pipeline
Processes: FHIR JSON, HL7 v2 messages, OCR'd PDFs, CSV files
Output: 6 bronze layer Parquet tables
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, current_timestamp,
    lit, regexp_extract, split, trim, when
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, DateType
import os

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Multi-Format-Healthcare-ETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    print("\n" + "="*80)
    print("STARTING MULTI-FORMAT HEALTHCARE DATA ETL PIPELINE")
    print("="*80 + "\n")

    # Base paths
    raw_base = "../../data/raw"
    bronze_base = "../../data/processed/bronze"

    # Ensure bronze directory exists
    os.makedirs(bronze_base, exist_ok=True)

    # ========================================================================
    # 1. PROCESS FHIR JSON DATA
    # ========================================================================
    print("📋 [1/4] Processing FHIR JSON files...")

    try:
        # Read FHIR bundles
        df_fhir_raw = spark.read.option("multiline", "true").json(f"{raw_base}/fhir/*.json")

        # Extract FHIR Patients
        df_patients_fhir = df_fhir_raw.select(
            explode("entry").alias("entry")
        ).filter(
            col("entry.resource.resourceType") == "Patient"
        ).select(
            col("entry.resource.id").alias("patient_id"),
            lit(None).cast(StringType()).alias("mrn"),
            col("entry.resource.name")[0]["family"].alias("family_name"),
            col("entry.resource.name")[0]["given"][0].alias("given_name"),
            col("entry.resource.gender").alias("gender"),
            to_timestamp(col("entry.resource.birthDate")).alias("birth_date"),
            col("entry.resource.address")[0]["city"].alias("city"),
            col("entry.resource.address")[0]["state"].alias("state"),
            lit("FHIR_API").alias("source_system"),
            current_timestamp().alias("ingested_at")
        )

        # Extract FHIR Observations
        df_observations_fhir = df_fhir_raw.select(
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
            lit("FHIR_API").alias("source_system"),
            current_timestamp().alias("ingested_at")
        )

        # Write FHIR tables
        df_patients_fhir.write.mode("overwrite").parquet(f"{bronze_base}/patients_fhir")
        df_observations_fhir.write.mode("overwrite").parquet(f"{bronze_base}/observations_fhir")

        print(f"   ✅ FHIR: {df_patients_fhir.count()} patients, {df_observations_fhir.count()} observations")

    except Exception as e:
        print(f"   ⚠️  FHIR processing failed: {str(e)}")

    # ========================================================================
    # 2. PROCESS HL7 v2 MESSAGES
    # ========================================================================
    print("\n📋 [2/4] Processing HL7 v2 messages...")

    try:
        # Read HL7 messages as text
        df_hl7_raw = spark.read.text(f"{raw_base}/hl7/*.hl7")

        # Extract Patient Demographics from PID segment
        df_patients_hl7 = df_hl7_raw.filter(
            col("value").contains("PID|")
        ).select(
            # Extract MRN from PID-3 (field 3)
            regexp_extract(col("value"), r"PID\|[^|]*\|[^|]*\|([^|^]*)", 1).alias("mrn"),
            # Extract family name from PID-5
            regexp_extract(col("value"), r"PID\|(?:[^|]*\|){4}([^^]*)", 1).alias("family_name"),
            # Extract given name from PID-5
            regexp_extract(col("value"), r"PID\|(?:[^|]*\|){4}[^^]*\^([^^]*)", 1).alias("given_name"),
            # Extract gender from PID-8
            regexp_extract(col("value"), r"PID\|(?:[^|]*\|){7}([^|]*)", 1).alias("gender"),
            lit("HL7_v2").alias("source_system"),
            current_timestamp().alias("ingested_at")
        ).filter(
            col("mrn") != ""
        )

        # Extract Observations from OBX segments
        df_observations_hl7 = df_hl7_raw.filter(
            col("value").contains("OBX|")
        ).select(
            # Extract LOINC code from OBX-3
            regexp_extract(col("value"), r"OBX\|[^|]*\|[^|]*\|([^^]*)", 1).alias("loinc_code"),
            # Extract observation name from OBX-3
            regexp_extract(col("value"), r"OBX\|[^|]*\|[^|]*\|[^^]*\^([^^]*)", 1).alias("observation_name"),
            # Extract value from OBX-5
            regexp_extract(col("value"), r"OBX\|(?:[^|]*\|){4}([^|]*)", 1).alias("value"),
            # Extract unit from OBX-6
            regexp_extract(col("value"), r"OBX\|(?:[^|]*\|){5}([^|]*)", 1).alias("unit"),
            lit("HL7_v2").alias("source_system"),
            current_timestamp().alias("ingested_at")
        ).filter(
            col("loinc_code") != ""
        )

        # Write HL7 tables
        df_patients_hl7.write.mode("overwrite").parquet(f"{bronze_base}/patients_hl7")
        df_observations_hl7.write.mode("overwrite").parquet(f"{bronze_base}/observations_hl7")

        print(f"   ✅ HL7: {df_patients_hl7.count()} patients, {df_observations_hl7.count()} observations")

    except Exception as e:
        print(f"   ⚠️  HL7 processing failed: {str(e)}")

    # ========================================================================
    # 3. PROCESS OCR'D PDF/TXT FILES
    # ========================================================================
    print("\n📋 [3/4] Processing OCR'd PDF text files...")

    try:
        # Read text files
        df_ocr_raw = spark.read.text(f"{raw_base}/pdfs/*.txt")

        # Extract lab results (Basic Metabolic Panel pattern)
        # This is a simplified extraction - production would use more sophisticated NLP
        df_labs_ocr = df_ocr_raw.filter(
            col("value").contains("mg/dL") | col("value").contains("mmol/L")
        ).select(
            # Extract MRN
            when(col("value").contains("MRN"),
                 regexp_extract(col("value"), r"MRN[:\s]+([0-9]+)", 1)
            ).alias("mrn"),
            # Extract test name (word before result)
            regexp_extract(col("value"), r"([A-Za-z0-9]+)\s+([0-9.]+)\s+(mg/dL|mmol/L)", 1).alias("test_name"),
            # Extract value
            regexp_extract(col("value"), r"([A-Za-z0-9]+)\s+([0-9.]+)\s+(mg/dL|mmol/L)", 2).alias("value"),
            # Extract unit
            regexp_extract(col("value"), r"([A-Za-z0-9]+)\s+([0-9.]+)\s+(mg/dL|mmol/L)", 3).alias("unit"),
            lit("OCR_PDF").alias("source_system"),
            current_timestamp().alias("ingested_at")
        ).filter(
            (col("test_name") != "") & (col("value") != "")
        )

        # Write OCR table
        df_labs_ocr.write.mode("overwrite").parquet(f"{bronze_base}/labs_ocr")

        print(f"   ✅ OCR: {df_labs_ocr.count()} lab results extracted")

    except Exception as e:
        print(f"   ⚠️  OCR processing failed: {str(e)}")

    # ========================================================================
    # 4. PROCESS CSV FILES
    # ========================================================================
    print("\n📋 [4/4] Processing CSV files...")

    try:
        # Define schema for deployment readiness
        readiness_schema = StructType([
            StructField("soldier_id", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("deployment_status", StringType(), True),
            StructField("medical_clearance", StringType(), True),
            StructField("dental_class", StringType(), True),
            StructField("last_pha_date", StringType(), True),
            StructField("bmi", StringType(), True),
            StructField("deployable", StringType(), True),
            StructField("notes", StringType(), True)
        ])

        # Read CSV
        df_readiness_raw = spark.read \
            .option("header", "true") \
            .schema(readiness_schema) \
            .csv(f"{raw_base}/csv/*.csv")

        # Clean and transform
        df_readiness = df_readiness_raw.select(
            col("soldier_id"),
            col("unit"),
            col("deployment_status"),
            when(col("medical_clearance") == "YES", True)
                .when(col("medical_clearance") == "NO", False)
                .otherwise(None).alias("medical_clearance"),
            col("dental_class").cast("int").alias("dental_class"),
            to_timestamp(col("last_pha_date"), "yyyy-MM-dd").alias("last_pha_date"),
            col("bmi").cast("double").alias("bmi"),
            when(col("deployable") == "TRUE", True)
                .when(col("deployable") == "FALSE", False)
                .otherwise(None).alias("deployable"),
            col("notes"),
            lit("MEDPROS_CSV").alias("source_system"),
            current_timestamp().alias("ingested_at")
        )

        # Write readiness table
        df_readiness.write.mode("overwrite").parquet(f"{bronze_base}/readiness_csv")

        print(f"   ✅ CSV: {df_readiness.count()} soldier readiness records")

    except Exception as e:
        print(f"   ⚠️  CSV processing failed: {str(e)}")

    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "="*80)
    print("ETL PIPELINE COMPLETE - Bronze Layer Created")
    print("="*80)
    print(f"\n📁 Output location: {bronze_base}/")
    print("\n📊 Tables created:")
    print("   1. patients_fhir/")
    print("   2. observations_fhir/")
    print("   3. patients_hl7/")
    print("   4. observations_hl7/")
    print("   5. labs_ocr/")
    print("   6. readiness_csv/")
    print("\n✅ Ready for dbt transformation (Silver/Gold layers)\n")

    spark.stop()

if __name__ == "__main__":
    main()
