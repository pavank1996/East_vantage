# Solution 1: SQL

import sqlite3
import pandas as pd

conn = sqlite3.connect("sales.db")

# Write a SQL query to get the total quantities of each item bought per customer aged 18-35

sql = """
with data as (
SELECT c.customer_id, c.age, s.sales_id, o.item_id, o.quantity i.item_name from Customer c
JOIN Sales s on c.customer_id = s.customer_id
JOIN Orders o on s.sales_id = o.sales_id
JOIN Items i on o.item_id = i.item_id
WHERE c.age BETWEEN 18 AND 35 AND o.quantity IS NOT NULL
)
select customer_id as customer, age, item_name as item , sum(quantity) as Quantity
from data
group by customer_id, age, item_name
order by customer_id, item_name
"""

#fetch result
cur = conn.cursor()
cur.execute(sql)
results = cur.fetchall()

# save the result with sep = ';'
df = pd.DataFrame(results, columns=['Customer', 'Age', 'Item', 'Quantity'])
df.to_csv('SQL_OUTPUT.csv', sep=';', index=False)

# ------------------------------------------------------------------------------------------------------

# Solution 2: Pandas
# Read the tables into pandas dataframes
customer = pd.read_sql("SELECT * FROM customer", conn)
sales = pd.read_sql("SELECT * FROM Sales", conn)
orders = pd.read_sql("SELECT * FROM Orders", conn)
items = pd.read_sql("SELECT * FROM Items", conn)

# Merge the dataframes
df = pd.merge(customer, sales, on="customer_id")
df = pd.merge(df, orders, on="sales_id")
df = pd.merge(df, items, on="item_id")

# Filter the group
df = df[(df["age"] >= 18) & (df["age"] <= 35) & (df["quantity"].notnull())]
df = df.groupby(["customer_id", "age", "item_name"])["quantity"].sum().reset_index()

# Sort
df = df.sort_values(by=["customer_id", "item_name"])

# save the result with sep = ';'
df.to_csv("PANDAS_OUPUT.csv", sep=";", index=False)

conn.close()

------------------------------------------------------------------------------------------------------
