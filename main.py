"""
ETL-Query script
"""

from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import run_crud_operations, query_frequent_soda, query_heart_disease

# Extract
print("Extracting data...")
extract()

# Transform and load
print("Transforming data...")
load()

# Query CRUD
print("CRUD on data...")
run_crud_operations()

# Query
print("Querying data...")
query_frequent_soda()
query_heart_disease()
