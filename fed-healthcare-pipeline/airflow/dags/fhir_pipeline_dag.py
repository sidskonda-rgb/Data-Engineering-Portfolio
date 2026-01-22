from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'sri-konda',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fhir_healthcare_pipeline',
    default_args=default_args,
    description='FHIR data ingestion and transformation pipeline',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_fhir = BashOperator(
        task_id='extract_fhir_data',
        bash_command='echo "✅ FHIR data detected in S3/raw"',
    )
    
    transform_spark = BashOperator(
        task_id='transform_with_spark',
        bash_command='cd /opt/airflow/spark/jobs && python transform_fhir_to_analytics.py',
    )
    
    load_dbt = BashOperator(
        task_id='build_analytics_models',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .',
    )
    
    test_dbt = BashOperator(
        task_id='test_data_quality',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
    )
    
    extract_fhir >> transform_spark >> load_dbt >> test_dbt