# To run this script in terminal run: - python import_csv.py or Run this file

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import os

from dotenv import load_dotenv

load_dotenv()

# Enter information about names and csv file path here
csv_file_name = 'Electric_Vehicle_Population_Data.csv'
db_name = "EV"
table_name = "ev_population"


# Reads data from .env file
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Get the current script's directory (absolute path)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Move up to the root directory (go up two levels)
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, os.pardir, os.pardir))

# Database connection details


CSV_FILE = os.path.join(ROOT_DIR, 'data', 'csv_files', csv_file_name)  # Path to your CSV file

# Step 1: Read CSV to get column names and data types
df = pd.read_csv(CSV_FILE)

# Step 2: Convert Pandas dtypes to MySQL-compatible types
dtype_mapping = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "VARCHAR(255)",
}

columns_sql = ", ".join(
    f"`{col}` {'TEXT' if col == 'fun_facts' else dtype_mapping.get(str(df[col].dtype), 'VARCHAR(255)')}"
    for col in df.columns
)

# Step 3: Connect to MySQL
conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD)
cursor = conn.cursor()

# Step 4: Drop database if it exists and create a new one
cursor.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
cursor.execute(f"CREATE DATABASE `{db_name}`")
print(f"Database `{db_name}` created successfully.")

# Select the database before creating the table
cursor.execute(f"USE `{db_name}`")

# Step 5: Drop table if it exists and create a new table dynamically
cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
create_table_query = f"CREATE TABLE `{table_name}` ({columns_sql});"
cursor.execute(create_table_query)
print(f"Table `{table_name}` created successfully.")

# Step 6: Insert data into the table using Pandas
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{db_name}")
df.to_sql(table_name, con=engine, if_exists="append", index=False)

print(f"Data imported successfully into `{table_name}`.")

# Step 7: Close connections
cursor.close()
conn.close()
