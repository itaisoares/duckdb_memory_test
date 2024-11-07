import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import uuid
import os

# Constants
NUM_USERS = 10_000_000
NUM_PRODUCTS = 5_000
NUM_ORDERS = 10_000_000
CHUNK_SIZE = 250_000  # Adjust the chunk size based on your memory capacity
DATA_FOLDER = 'data'

# Ensure the data folder exists
os.makedirs(DATA_FOLDER, exist_ok=True)

# Function to generate users data in chunks
def generate_users_chunk(start, end):
    return pd.DataFrame({
        'id': np.arange(start, end + 1),
        'uuid': [str(uuid.uuid4()) for _ in range(end - start + 1)],
        'name': ['User ' + str(i) for i in range(start, end + 1)],
        'birthdate': pd.to_datetime(np.random.randint(
            np.datetime64('1970-01-01').astype(int),
            np.datetime64('2000-01-01').astype(int),
            end - start + 1), unit='D'),
        'address': ['Address ' + str(i) for i in range(start, end + 1)],
        'telephone': ['Telephone ' + str(i) for i in range(start, end + 1)]
    })

# Function to generate products data
def generate_products():
    return pd.DataFrame({
        'id': np.arange(1, NUM_PRODUCTS + 1),
        'name': ['Product ' + str(i) for i in range(1, NUM_PRODUCTS + 1)],
        'description': ['Description for product ' + str(i) for i in range(1, NUM_PRODUCTS + 1)]
    })

# Function to generate orders data in chunks
def generate_orders_chunk(start, end):
    return pd.DataFrame({
        'user_id': np.random.randint(1, NUM_USERS + 1, end - start + 1),
        'product_id': np.random.randint(1, NUM_PRODUCTS + 1, end - start + 1),
        'price': np.random.rand(end - start + 1) * 100,
        'quantity': np.random.randint(1, 11, end - start + 1),
        'order_timestamp': pd.to_datetime(np.random.randint(
            np.datetime64('2020-01-01').astype(int),
            np.datetime64('2023-01-01').astype(int),
            end - start + 1), unit='D')
    })

# Write users data to Parquet in chunks
user_files = []
for start in range(1, NUM_USERS + 1, CHUNK_SIZE):
    end = min(start + CHUNK_SIZE - 1, NUM_USERS)
    users_chunk = generate_users_chunk(start, end)
    file_path = os.path.join(DATA_FOLDER, f'users_{start}_{end}.parquet')
    user_files.append(file_path)
    table = pa.Table.from_pandas(users_chunk)
    pq.write_table(table, file_path)

# Write products data to Parquet
products = generate_products()
products_file = os.path.join(DATA_FOLDER, 'products.parquet')
products.to_parquet(products_file, index=False)

# Write orders data to Parquet in chunks
order_files = []
for start in range(1, NUM_ORDERS + 1, CHUNK_SIZE):
    end = min(start + CHUNK_SIZE - 1, NUM_ORDERS)
    orders_chunk = generate_orders_chunk(start, end)
    file_path = os.path.join(DATA_FOLDER, f'orders_{start}_{end}.parquet')
    order_files.append(file_path)
    table = pa.Table.from_pandas(orders_chunk)
    pq.write_table(table, file_path)

# Merge all user files into a single Parquet file using streaming
with pq.ParquetWriter(os.path.join(DATA_FOLDER, 'users.parquet'), pq.read_table(user_files[0]).schema) as writer:
    for file in user_files:
        table = pq.read_table(file)
        writer.write_table(table)

# Merge all order files into a single Parquet file using streaming
with pq.ParquetWriter(os.path.join(DATA_FOLDER, 'orders.parquet'), pq.read_table(order_files[0]).schema) as writer:
    for file in order_files:
        table = pq.read_table(file)
        writer.write_table(table)

# Delete temporary user files
for file in user_files:
    os.remove(file)

# Delete temporary order files
for file in order_files:
    os.remove(file)

print("Parquet files generated, merged, and temporary files deleted successfully.")