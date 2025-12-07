from .base_scheduler import BaseScheduler
from cannabis_data.sql_engine.db_manager import DBManager
from cannabis_data.analysis.analyzer import Analyzer
from cannabis_data.analysis.association_rules import AssociationRules
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class AnalysisScheduler(BaseScheduler):
    def __init__(self, db=None, **kwargs):
        super().__init__(**kwargs)
        self.db = db or DBManager()

    def _run(self):
        logger.info("Starting analysis")
        df = self.db.read_sql("SELECT * FROM retail_sales_cleaned")
        analyzer = Analyzer(df)
        totals = {
            "total_revenue": analyzer.total_revenue(),
            "avg_basket": analyzer.average_basket_size(),
            "top_products": analyzer.top_products(10),
            "store_rank": analyzer.store_ranking(10)
        }

        # association rules (needs transaction-level data)
        if "transaction_id" in df.columns and "sku" in df.columns:
            tx = df[["transaction_id", "sku"]].drop_duplicates()
            assoc = AssociationRules(tx)
            rules = assoc.mine_rules(min_support=0.01, min_confidence=0.25)
        else:
            rules = pd.DataFrame()

        logger.info("Analysis finished")

        # As a demo, simple artifacts written back to DB as csv-styled tables
        self.db.write_df(pd.DataFrame([{"metric":"total_revenue","value":totals["total_revenue"]}]), "analysis_metrics", if_exists="replace")
        if not rules.empty:
            self.db.write_df(rules.reset_index(), "analysis_rules", if_exists="replace")
        return totals

    def run(self):
        return self._with_retry(self._run)
