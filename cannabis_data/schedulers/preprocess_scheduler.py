from .base_scheduler import BaseScheduler
from cannabis_data.sql_engine.db_manager import DBManager
from cannabis_data.cleaning.preprocessor import Preprocessor
import logging

logger = logging.getLogger(__name__)

class PreprocessScheduler(BaseScheduler):

    def __init__(self, db=None, **kwargs):
        super().__init__(**kwargs)
        self.db = db or DBManager()

    def _preprocess(self):
        logger.info("Starting preprocess")

        df = self.db.read_sql("SELECT * FROM retail_sales")
        pre = Preprocessor(df)
        cleaned = pre.run_all()

        self.db.write_df(cleaned, "retail_sales_cleaned", if_exists="replace")
        logger.info("Preprocess finished, rows: %s", len(cleaned))
        return len(cleaned)

    def run(self):
        return self._with_retry(self._preprocess)
