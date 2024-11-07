import duckdb
import os
import time

def get_file_size_in_gb(file_path):
    file_size = os.path.getsize(file_path)
    return file_size / (1024 * 1024 * 1024) #GB

users_parquet_path = './data/users.parquet'
orders_parquet_path = './data/orders.parquet'

con = duckdb.connect()

def run_query(query:str):
    print(f"DuckDB version: {duckdb.__version__}")
    print(f"Size of users.parquet: {get_file_size_in_gb(users_parquet_path):.2f} GB")
    print(f"Size of orders.parquet: {get_file_size_in_gb(orders_parquet_path):.2f} GB")
    start_time = time.time()
    result = con.execute(query).fetchdf()
    end_time = time.time()
    print(f"Query execution time: {end_time - start_time:.2f} seconds")