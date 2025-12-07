import pandas as pd
from cannabis_data.common.exceptions import AnalyzerError

class Analyzer:
    """
    Lightweight analysis platform with a few standard analyses:
      - total revenue
      - monthly time-series
      - average basket size
      - top products
      - store ranking
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def total_revenue(self):
        return float(self.df["sales"].sum())

    def monthly_sales(self, freq="M"):
        df = self.df.set_index("date")
        return df["sales"].resample(freq).sum()

    def average_basket_size(self):

        if "transaction_id" in self.df.columns:
            trx = self.df.groupby("transaction_id")["sales"].sum()
            return float(trx.mean())
        else:
            # fallback: sales / transactions_count if available
            return float(self.df["sales"].sum() / max(1, self.df.get("transaction_count", 1).sum()))

    def top_products(self, n=10):
        return self.df.groupby(["sku", "product_name"])["sales"].sum().sort_values(ascending=False).head(n)

    def store_ranking(self, n=20):
        return self.df.groupby("store_id")["sales"].sum().sort_values(ascending=False).head(n)
    
    def run_all(self):
        try:
            self.total_revenue()
            self.monthly_sales()
            self.average_basket_size()
            self.top_products()
            self.store_ranking()

            return self.df
        except Exception as e:
            raise AnalyzerError(str(e))
