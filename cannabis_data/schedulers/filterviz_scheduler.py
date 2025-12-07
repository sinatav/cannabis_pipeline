from .base_scheduler import BaseScheduler
from cannabis_data.sql_engine.db_manager import DBManager
from cannabis_data.filtering.visualizer import Visualizer
from cannabis_data.filtering.filterer import Filterer
import logging

logger = logging.getLogger(__name__)

class FilterVizScheduler(BaseScheduler):
    def __init__(self, db=None, **kwargs):
        super().__init__(**kwargs)
        self.db = db or DBManager()

    def _run(self):
        logger.info("Start filter & visualize")
        df = self.db.read_sql("SELECT * FROM retail_sales_cleaned")
        
        # quick filters example
        f = Filterer(df)
        last_6m = f.filter_by_date_range(start_date=None, end_date=None)  # you can parametrize
        
        # show monthly series
        Visualizer.plot_time_series(last_6m, title="All stores — monthly sales")
        Visualizer.plot_top_products(last_6m, n=12)
        logger.info("Filter & visualize done")
        return True

    def run(self):
        return self._with_retry(self._run)
