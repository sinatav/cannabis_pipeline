import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules

class AssociationRules:

    def __init__(self, transactions_df: pd.DataFrame):

        self.transactions_df = transactions_df

    def _basket_encoding(self):
        # Building one-hot encoded basket matrix
        basket = (self.transactions_df
                  .assign(count=1)
                  .pivot_table(index='transaction_id', columns='sku', values='count', fill_value=0))
        return basket

    def mine_rules(self, min_support=0.01, min_confidence=0.3):
        basket = self._basket_encoding()
        freq_itemsets = apriori(basket, min_support=min_support, use_colnames=True)
        rules = association_rules(freq_itemsets, metric="confidence", min_threshold=min_confidence)
        return rules.sort_values(by="lift", ascending=False)
