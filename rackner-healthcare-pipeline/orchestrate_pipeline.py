"""
Healthcare Data Pipeline Orchestrator
Demonstrates DAG-based workflow orchestration pattern
Production: Apache Airflow DAG

Execution Flow:
1. Bronze Layer: Multi-format ETL (FHIR, HL7, OCR, CSV) -> Parquet
2. Silver/Gold Layer: dbt transformations -> DuckDB analytics tables
3. Data Quality: dbt tests validation
"""
import subprocess
import sys
from datetime import datetime
from pathlib import Path

def run_command(description, command, critical=True):
    """Execute a shell command with error handling"""
    print(f"\n{'='*80}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {description}")
    print(f"{'='*80}")

    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )
        if result.stdout:
            print(result.stdout)
        print(f"[OK] {description} - SUCCESS")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] {description} - FAILED")
        if e.stderr:
            print(e.stderr)
        if e.stdout:
            print(e.stdout)
        if critical:
            print(f"\n[CRITICAL] Pipeline halted due to critical failure")
            sys.exit(1)
        return False

def main():
    """Main orchestration flow"""
    print("""
    ========================================================================
    Healthcare Data Pipeline - End-to-End Orchestration
    (Production: Apache Airflow DAG)
    ========================================================================

    Architecture: Medallion (Bronze -> Silver -> Gold)
    Tech Stack: Pandas ETL + dbt + DuckDB

    """)

    start_time = datetime.now()

    # Define pipeline steps
    steps = [
        {
            "name": "TASK 1: Spark/Pandas ETL - Bronze Layer",
            "command": "venv\\Scripts\\python spark\\jobs\\transform_messy_healthcare_data_pandas.py",
            "critical": True,
            "description": "Multi-format data ingestion (FHIR, HL7, OCR, CSV) -> Parquet"
        },
        {
            "name": "TASK 2: dbt Build - Silver/Gold Layers",
            "command": "cd dbt && ..\\venv\\Scripts\\dbt run --profiles-dir .",
            "critical": True,
            "description": "SQL transformations: staging models (views) + marts (tables)"
        },
        {
            "name": "TASK 3: dbt Tests - Data Quality Validation",
            "command": "cd dbt && ..\\venv\\Scripts\\dbt test --profiles-dir .",
            "critical": False,
            "description": "Automated data quality checks (uniqueness, nullability, referential integrity)"
        }
    ]

    # Execute pipeline
    success_count = 0
    for step in steps:
        success = run_command(
            step["name"],
            step["command"],
            critical=step["critical"]
        )
        if success:
            success_count += 1

    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"\n{'='*80}")
    print("PIPELINE EXECUTION SUMMARY")
    print(f"{'='*80}")
    print(f"Total Steps: {len(steps)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {len(steps) - success_count}")
    print(f"Duration: {duration:.2f} seconds")
    print(f"\nOutput Artifacts:")
    print(f"  - Bronze Layer: data/processed/bronze/ (6 Parquet tables)")
    print(f"  - Silver/Gold Layer: data/processed/analytics.duckdb (3 models)")
    print(f"\n[OK] PIPELINE COMPLETE\n")

if __name__ == "__main__":
    main()
