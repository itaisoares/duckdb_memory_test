import duckdb
import pandas as pd

# Connect to an in-memory DuckDB instance
con = duckdb.connect()

# Query to join users and orders, and generate a new number using ROW_NUMBER()
query = """
WITH joined_data AS (
    SELECT
        u.id AS user_id,
        u.uuid,
        u.name,
        u.birthdate,
        u.address,
        u.telephone,
        o.user_id AS order_user_id,
        o.product_id,
        o.price,
        o.quantity,
        o.order_timestamp
    FROM 'data/users.parquet' AS u
    JOIN 'data/orders.parquet' AS o
    ON u.id = o.user_id
)
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY user_id, order_timestamp) AS new_number
FROM joined_data
"""

# Execute the query and fetch the result
result = con.execute(query).fetchdf()

# Print the result
print(result.head())