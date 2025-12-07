from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from cannabis_data.schedulers.preprocess_scheduler import PreprocessScheduler
from cannabis_data.config import AIRFLOW_RETRIES, AIRFLOW_RETRY_DELAY_MINUTES

default_args = {
    "owner": "cannabis_pipeline",
    "retries": AIRFLOW_RETRIES,
    "retry_delay": timedelta(minutes=AIRFLOW_RETRY_DELAY_MINUTES),
}

with DAG(dag_id="preprocess_retail_sales",
         start_date=datetime(2023,1,1),
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:

    def _run_preprocess():
        sched = PreprocessScheduler()
        sched.run()

    task = PythonOperator(
        task_id="preprocess_task",
        python_callable=_run_preprocess
    )
