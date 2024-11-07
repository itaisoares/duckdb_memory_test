import duckdb
import pandas as pd

# Connect to an in-memory DuckDB instance
con = duckdb.connect()

# Query to join users and orders, calculate age, bucket the ages, and sum the orders
query = """
WITH user_ages AS (
    SELECT
        id,
        DATE_PART('year', AGE(birthdate)) AS age
    FROM './data/users.parquet'
),
age_buckets AS (
    SELECT
        id,
        CASE
            WHEN age BETWEEN 0 AND 10 THEN '0-10'
            WHEN age BETWEEN 11 AND 20 THEN '11-20'
            WHEN age BETWEEN 21 AND 30 THEN '21-30'
            WHEN age BETWEEN 31 AND 40 THEN '31-40'
            WHEN age BETWEEN 41 AND 50 THEN '41-50'
            WHEN age BETWEEN 51 AND 60 THEN '51-60'
            WHEN age BETWEEN 61 AND 70 THEN '61-70'
            WHEN age BETWEEN 71 AND 80 THEN '71-80'
            WHEN age BETWEEN 81 AND 90 THEN '81-90'
            ELSE '91+'
        END AS age_bucket
    FROM user_ages
)
SELECT
    age_bucket,
    SUM(o.price * o.quantity) AS total_orders
FROM age_buckets u
JOIN './data/orders.parquet' o
ON u.id = o.user_id
GROUP BY age_bucket
ORDER BY age_bucket
"""

# Execute the query and fetch the result
result = con.execute(query).fetchdf()

# Print the result
print(result)