import os
import sys
from base import run_query

os.environ["MALLOC_CONF"] = (
    f"narenas:{os.cpu_count()},lg_chunk:21,background_thread:true,dirty_decay_ms:10000,muzzy_decay_ms:10000"
)

folder_size = sys.argv[1] if len(sys.argv) > 1 else ''
users_parquet_path = f'./data/{folder_size}/users.parquet'
orders_parquet_path = f'./data/{folder_size}/orders.parquet'

query = f"""
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
    FROM '{users_parquet_path}' AS u
    JOIN '{orders_parquet_path}' AS o
    ON u.id = o.user_id
)
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_timestamp) AS new_number
FROM joined_data
"""

print("With window function:")
run_query(query, folder_size)
