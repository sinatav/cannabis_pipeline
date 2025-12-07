import os

# DB URI
DB_URI = os.getenv("CANNABIS_DB_URI", "sqlite:///cannabis_sales.db")

# Local data folder
DATA_DIR = os.getenv("CANNABIS_DATA_DIR", "data/")

# Cannlytics dataset
CANNLYTICS_DATASET = os.getenv("CANNLYTICS_DATASET", "wa_cannabis_sales")

AIRFLOW_RETRIES = int(os.getenv("AIRFLOW_RETRIES", "32"))
AIRFLOW_RETRY_DELAY_MINUTES = int(os.getenv("AIRFLOW_RETRY_DELAY_MINUTES", "1"))
