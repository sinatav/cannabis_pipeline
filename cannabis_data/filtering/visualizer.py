import matplotlib.pyplot as plt
import pandas as pd

class Visualizer:

    @staticmethod
    def plot_time_series(df: pd.DataFrame, date_col="date", value_col="sales", resample="M", title=None):
        s = df.set_index(date_col)[value_col].resample(resample).sum()
        s.plot()
        plt.title(title or "Time series")
        plt.xlabel("Date")
        plt.ylabel(value_col)
        plt.tight_layout()
        plt.show()

    @staticmethod
    def plot_top_products(df: pd.DataFrame, n=10):
        series = df.groupby("sku")["sales"].sum().sort_values(ascending=False).head(n)
        series.plot(kind="bar")
        plt.title("Top Products by Sales")
        plt.tight_layout()
        plt.show()
