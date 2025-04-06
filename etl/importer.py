import os
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
from utilities.decorators import time_count

load_dotenv()

db_name = "jeopardy_2"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, os.pardir))

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
USE_INDEX_LABEL = os.getenv('USE_INDEX_LABEL', 'false').lower() == 'true'



def create_database():
    """Create the database if it does not exist."""
    try:
        with mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                print(f"Database `{db_name}` checked/created.")
    except mysql.connector.Error as err:
        print(f"Error creating database: {err}")


def clean_dataframe(df):
    df.drop_duplicates(inplace=True)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    object_cols = df.select_dtypes(include=["object"]).columns
    df[object_cols] = df[object_cols].apply(lambda col: col.str.strip().str.replace(r'[^\x00-\x7F]+', '', regex=True))

    df.dropna(thresh=len(df) * 0.5, axis=1, inplace=True)

    if 'value' in df.columns:
        df['value'] = pd.to_numeric(df['value'].replace(r'[\$,]', '', regex=True), errors='coerce')

    if 'air_date' in df.columns:
        df['air_date'] = pd.to_datetime(df['air_date'], errors='coerce')

    return df



create_database()

engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{db_name}")


def load_file(file_path, file_type):
    if file_type == "csv":
        return pd.read_csv(file_path)
    elif file_type == "json":
        return pd.read_json(file_path)
    elif file_type == "xlsx":
        return pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

@time_count
def import_file(file_name, file_type="csv", chunk_size=5000):
    """
    Dynamically import data from a CSV, JSON, or Excel file into a MySQL table.

    :param file_name: Name of the file (without path)
    :param file_type: One of "csv", "json", or "xlsx"
    :param chunk_size: Number of rows inserted at a time
    """
    file_path = os.path.join(ROOT_DIR, 'data_files', file_name)

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    try:
        df = load_file(file_path, file_type)
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    df = clean_dataframe(df)

    if df.empty:
        print(f"No data found in {file_type.upper()} file.")
        return

    print(f"Dataframe has {len(df)} rows.")
    print(df.head())

    table_name = os.path.splitext(file_name)[0].lower()

    print(f"Importing {file_path} into {table_name}")

    metadata = MetaData()
    metadata.reflect(bind=engine)

    if table_name not in metadata.tables:
        df.head(0).to_sql(table_name, engine, if_exists="fail", index=False)
        print(f"Table {table_name} created.")

    try:
        print(f"Inserting data into {table_name}...")
        df.to_sql(table_name, engine, if_exists="append", index=False, chunksize=chunk_size, method="multi")
        print(f"Data successfully inserted into {table_name}.")
    except OperationalError as e:
        print(f"Error inserting data: {e}")
        print(df.head())


# Examples:
import_file("JEOPARDY_CSV.csv", file_type="csv")
# import_file("JEOPARDY_DATA.json", file_type="json")
# import_file("JEOPARDY_SHEET.xlsx", file_type="xlsx")
