from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from cannabis_data.schedulers.filterviz_scheduler import FilterVizScheduler
from cannabis_data.config import AIRFLOW_RETRIES, AIRFLOW_RETRY_DELAY_MINUTES

default_args = {
    "owner": "cannabis_pipeline",
    "retries": AIRFLOW_RETRIES,
    "retry_delay": timedelta(minutes=AIRFLOW_RETRY_DELAY_MINUTES),
}

with DAG(dag_id="filter_visualize_sql",
         start_date=datetime(2023,1,1),
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:

    def _run_filterviz():
        sched = FilterVizScheduler()
        sched.run()

    task = PythonOperator(
        task_id="filterviz_task",
        python_callable=_run_filterviz
    )
