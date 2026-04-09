import pandas as pd
import sqlite3

#connect to producs db
connection = sqlite3.connect("products.db")

#query the db
query_one = """
SELECT title, category, price, discounted_price
FROM products
LIMIT 10
"""

result = pd.read_sql_query(query_one, connection)

print(result)

query_two = """
SELECT category, AVG(price) AS avg_price
FROM products
GROUP BY category
ORDER BY avg_price DESC
LIMIT 10
"""

result = pd.read_sql_query(query_two, connection)

print(result)
# close connection
connection.close()