from .base_scheduler import BaseScheduler
from cannabis_data.loading.cannlytics_loader import CannlyticsLoader
from cannabis_data.sql_engine.db_manager import DBManager
import logging

logger = logging.getLogger(__name__)

class IngestScheduler(BaseScheduler):

    def __init__(self, loader=None, db_manager=None, **kwargs):
        super().__init__(**kwargs)
        self.loader = loader or CannlyticsLoader()
        self.db = db_manager or DBManager()

    def _ingest(self):
        logger.info("Starting ingest")
        df = self.loader.load()
        # basic sanity: ensure date col exists or cast if necessary
        if "date" not in df.columns:
            raise ValueError("No date column in loaded DataFrame")

        self.db.write_df(df, "retail_sales", if_exists="append")
        logger.info("Ingest finished, rows written: %s", len(df))
        return len(df)

    def run(self):
        return self._with_retry(self._ingest)
