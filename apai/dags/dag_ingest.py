from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from cannabis_data.schedulers.ingest_scheduler import IngestScheduler
from cannabis_data.config import AIRFLOW_RETRIES, AIRFLOW_RETRY_DELAY_MINUTES

default_args = {
    "owner": "cannabis_pipeline",
    "depends_on_past": False,
    "retries": AIRFLOW_RETRIES,
    "retry_delay": timedelta(minutes=AIRFLOW_RETRY_DELAY_MINUTES),
}

with DAG(dag_id="ingest_retail_sales",
         start_date=datetime(2023,1,1),
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:

    def _run_ingest():
        sched = IngestScheduler()
        sched.run()

    task = PythonOperator(
        task_id="ingest_task",
        python_callable=_run_ingest
    )
