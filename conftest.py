
from dotenv import load_dotenv
import os
import pytest
import mysql.connector
from mysql.connector import Error

load_dotenv()
@pytest.fixture(scope="session")
def db_connection():
    """Fixture to establish a database connection and clean up after tests."""
    try:
        config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME', "classicmodels"),
            'raise_on_warnings': True,
            'auth_plugin': 'caching_sha2_password'
        }
        connection = mysql.connector.connect(**config)
        yield connection

    except Error as e:
        pytest.fail(f"Error connecting to the database: {e}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("Database connection closed.")


@pytest.fixture(scope="function")
def db_cursor(db_connection):
    """Fixture to get a fresh database cursor for each test."""
    cursor = db_connection.cursor(dictionary=False)
    yield cursor
    db_connection.rollback()
    cursor.close()

