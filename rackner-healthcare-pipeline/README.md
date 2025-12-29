# Fed Healthcare Data Pipeline - FHIR to Analytics

## Overview
Production-grade ETL/ELT pipeline demonstrating healthcare data interoperability using FHIR standards, Spark transformations, and dbt modeling - aligned with federal health IT modernization practices.

## Architecture
```
FHIR JSON (raw/) 
  ↓ [Apache Airflow Orchestration]
  ↓ [PySpark Transformation]
Bronze Layer (Parquet)
  ↓ [dbt Models + Tests]
Analytics (Gold Layer)
```

## Tech Stack
- **Orchestration**: Apache Airflow (DAG-based scheduling)
- **Transform**: Apache Spark (distributed processing)
- **Modeling**: dbt (version-controlled SQL transformations)
- **Standards**: FHIR (healthcare interoperability)
- **Storage**: Parquet (columnar format for analytics)
- **Testing**: dbt data quality tests

## Data Pipeline

### Ingestion (Bronze)
- **Input**: FHIR Bundle JSON (Patients + Observations)
- **Processing**: PySpark schema parsing and normalization
- **Output**: Parquet tables with audit timestamps

### Transformation (Silver → Gold)
- **dbt staging models**: Clean, type, deduplicate
- **dbt marts**: Join patients + observations for analytics
- **Data quality**: Uniqueness, nullability, referential integrity tests

## Quick Start
```bash
# 1. Run Spark transformation
cd spark/jobs && python transform_fhir_to_analytics.py

# 2. Build dbt models
cd dbt && dbt run && dbt test

# 3. Start Airflow (Docker)
cd airflow && docker-compose up
# Access UI: http://localhost:8080
```

## Key Features Demonstrated
✅ **Healthcare Standards**: FHIR resource parsing (Patient, Observation)  
✅ **ETL/ELT Pattern**: Spark for heavy lifting, dbt for modeling  
✅ **Data Governance**: Schema versioning, audit columns, data lineage  
✅ **Orchestration**: Airflow DAG with retries and dependencies  
✅ **Testing**: Automated data quality validation  

## Military/Federal Relevance
This pipeline architecture mirrors DoD/VA health IT modernization efforts:
- FHIR interoperability (MHS GENESIS, VA EHRM)
- Distributed processing for large-scale clinical data
- Audit-ready data lineage and governance
- Modular, version-controlled transformations

## Author
**Sri Konda** | Data Engineer | Active DoD Secret Clearance  
[LinkedIn](#) | [Portfolio](#)