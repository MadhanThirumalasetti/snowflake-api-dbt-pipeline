"""
salesforce_ingestion_dag.py

Daily Salesforce CRM → Snowflake ingestion pipeline.
Pulls Opportunities via REST API, loads to S3/Snowflake stage,
runs dbt transformations, and validates with Great Expectations
before marking data as business-ready.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from ingestion.salesforce_client import SalesforceClient
from ingestion.snowflake_loader import SnowflakeStageLoader


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["tmadhan0063@gmail.com"],
}


with DAG(
    dag_id="salesforce_opportunities_ingestion",
    description="Daily Salesforce Opportunities → Snowflake via S3 stage",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["salesforce", "ingestion", "snowflake"],
) as dag:

    def ingest_opportunities(**context):
        client = SalesforceClient(
            client_id="{{ var.value.sf_client_id }}",
            client_secret="{{ var.value.sf_client_secret }}",
            username="{{ var.value.sf_username }}",
            password="{{ var.value.sf_password }}",
            instance_url="{{ var.value.sf_instance_url }}",
        )
        loader = SnowflakeStageLoader(
            s3_bucket="{{ var.value.s3_bucket }}",
            s3_prefix="salesforce",
            snowflake_conn={
                "account": "{{ var.value.sf_account }}",
                "user": "{{ var.value.sf_user }}",
                "password": "{{ var.value.sf_password_snow }}",
                "warehouse": "INGESTION_WH",
                "database": "RAW",
                "schema": "SALESFORCE",
            },
        )
        total = 0
        for batch in client.fetch_opportunities(batch_size=200):
            loader.load(batch, entity="opportunities", target_table="salesforce_opportunities_raw")
            total += len(batch)
        context["ti"].xcom_push(key="records_loaded", value=total)

    ingest_task = PythonOperator(
        task_id="ingest_salesforce_opportunities",
        python_callable=ingest_opportunities,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_salesforce_models",
        bash_command=(
            "cd /opt/dbt && "
            "dbt run --select staging.stg_salesforce_opportunities+ "
            "--target prod --profiles-dir /opt/dbt/profiles"
        ),
    )

    def run_ge_checkpoint(**context):
        import great_expectations as ge
        context_ge = ge.get_context()
        result = context_ge.run_checkpoint(checkpoint_name="salesforce_opportunities_checkpoint")
        if not result["success"]:
            failed = [
                k for k, v in result["run_results"].items()
                if not v["validation_result"]["success"]
            ]
            raise ValueError(f"Great Expectations validation failed: {failed}")

    ge_checkpoint = PythonOperator(
        task_id="validate_ge_checkpoint",
        python_callable=run_ge_checkpoint,
    )

    dbt_test = BashOperator(
        task_id="dbt_test_salesforce_models",
        bash_command=(
            "cd /opt/dbt && "
            "dbt test --select staging.stg_salesforce_opportunities+ "
            "--target prod --profiles-dir /opt/dbt/profiles"
        ),
    )

    ingest_task >> dbt_run >> ge_checkpoint >> dbt_test
