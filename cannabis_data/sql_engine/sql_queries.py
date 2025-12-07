# Lightweight query library. Expand as needed.

GET_MONTHLY_SALES = """
SELECT strftime('%Y-%m', date) AS year_month,
       SUM(sales) AS total_sales,
       COUNT(DISTINCT transaction_id) AS transactions
FROM retail_sales
GROUP BY year_month
ORDER BY year_month;
"""

GET_TOP_PRODUCTS = """
SELECT sku, product_name, SUM(sales) AS total_sales, SUM(quantity) as total_units
FROM retail_sales
GROUP BY sku, product_name
ORDER BY total_sales DESC
LIMIT :limit;
"""

GET_STORE_RANKING = """
SELECT store_id, SUM(sales) AS total_sales, COUNT(DISTINCT transaction_id) AS transactions
FROM retail_sales
GROUP BY store_id
ORDER BY total_sales DESC;
"""
