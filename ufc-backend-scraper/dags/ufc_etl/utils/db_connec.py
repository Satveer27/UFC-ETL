import pyodbc
import time
import os
from dotenv import load_dotenv

def connect_to_database():
    attempt = 0
    load_dotenv()
    driver = os.getenv('DB_DRIVER')
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    port = os.getenv('DB_PORT')

    conn_str = f'Driver={driver};Server=tcp:{server},{port};Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    while attempt< 5:
        try:

            conn = pyodbc.connect(conn_str)
            print("Connection successful!")
            return conn
        
        except Exception as e:
            attempt += 1
            print(e)
            if attempt<5:
                print("Retrying to connect to database...")
                time.sleep(2)
            else:
                print("All attempts failed and could not connect to database")
                raise(e)