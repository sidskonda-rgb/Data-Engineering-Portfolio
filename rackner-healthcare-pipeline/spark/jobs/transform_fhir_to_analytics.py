from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, current_timestamp
import json

def main():
    spark = SparkSession.builder \
        .appName("FHIR-to-Analytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Read raw FHIR JSON
    df_raw = spark.read.option("multiline", "true").json("../data/raw/*.json")
    
    # Extract patients
    df_patients = df_raw.select(
        explode("entry").alias("entry")
    ).filter(
        col("entry.resource.resourceType") == "Patient"
    ).select(
        col("entry.resource.id").alias("patient_id"),
        col("entry.resource.name")[0]["family"].alias("family_name"),
        col("entry.resource.name")[0]["given"][0].alias("given_name"),
        col("entry.resource.gender").alias("gender"),
        to_timestamp(col("entry.resource.birthDate")).alias("birth_date"),
        current_timestamp().alias("ingested_at")
    )
    
    # Extract observations (vitals/labs)
    df_obs = df_raw.select(
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
        current_timestamp().alias("ingested_at")
    )
    
    # Write to parquet (bronze layer)
    df_patients.write.mode("overwrite").parquet("../data/processed/bronze/patients")
    df_obs.write.mode("overwrite").parquet("../data/processed/bronze/observations")
    
    print(f"✅ Processed {df_patients.count()} patients, {df_obs.count()} observations")
    
    spark.stop()

if __name__ == "__main__":
    main()