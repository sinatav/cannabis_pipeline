import pandas as pd

class Filterer:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def filter_by_date_range(self, start_date=None, end_date=None):
        df = self.df
        if start_date is not None:
            df = df[df["date"] >= pd.to_datetime(start_date)]
        if end_date is not None:
            df = df[df["date"] <= pd.to_datetime(end_date)]
        return df

    def filter_by_store(self, store_id):
        return self.df[self.df["store_id"] == store_id]

    def filter_by_category(self, category_name):
        return self.df[self.df["category"] == category_name]
