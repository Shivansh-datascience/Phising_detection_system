import pandas as pd
import pymysql
import os
from dotenv import load_dotenv
import sys
import os

#loading the current file with current directory environment variable
load_dotenv()


MYSQL_USERNAME = os.getenv(key="MYSQL_USERNAME")  #fetching MYSQL USERNAME FROM ENVIRONMENT VARIABLE
MYSQL_PASSWORD = os.getenv(key="MYSQL_PASSWORD")  #fetching MYSQL PASSWORD FROM ENVIRONMENT VARIABLE
MYSQL_DATABASE = os.getenv(key="MYSQL_DATABASE")  #fetching MYSQL DATABASE FROM ENVIRONMENT VARIABLE
MYSQL_HOST = os.getenv(key="MYSQL_HOST")  #fetching MYSQL HOST  FROM ENVIRONMENT VARIABLE
MYSQL_PORT = os.getenv(key="MYSQL_PORT")  #fetching MYSQL PORT FROM ENVIRONMENT VARIABLE

""" Connecting to database"""
try:
    connection = pymysql.connect(
        host=MYSQL_HOST,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=int(MYSQL_PORT),
        user=MYSQL_USERNAME,
        autocommit=True)
except pymysql.DatabaseError as database_error:
    raise database_error

#creating an cursor object for connection
def create_cursor_connection(connection):
    cursor_connection = connection.cursor()
    return cursor_connection

def fetch_and_save_to_csv(cursor_connection, sql_query, csv_filename):
    try:
        cursor_connection.execute(sql_query)
        column_names = [desc[0] for desc in cursor_connection.description]   #getting column name
        records = cursor_connection.fetchall()  # Fetch all records
        
        # Convert to DataFrame and save as CSV
        df = pd.DataFrame(records, columns=column_names)
        df.to_csv(csv_filename, index=False, encoding="utf-8")
        print(f"Data saved to {csv_filename}")
    except Exception as e:
        raise RuntimeError(f" Error fetching data: {e}")
    
#if connection is succesfull
if connection:
    cursor_connection = create_cursor_connection(connection)  #creating cursor object for executing the sql queries
    
    """ SQL query for fetching the Customer details"""
    customer_table = os.getenv(key="MYSQL_TABLE_1")
    customer_sql_query = f" SELECT * FROM {customer_table}" #sql query for fetching the records for customer table
    
    """ SQL Query for fetching phishing details """
    phishing_table = os.getenv(key="MYSQL_TABLE_2")
    phishing_sql_query = f"SELECT * FROM {phishing_table}" #sql query for fething the records for phising table

    customer_file_path = "D:\Phishing_detection_system\datasets\customer_details.csv"
    phishing_file_path = "D:\Phishing_detection_system\datasets\phishing_details.csv"

    fetch_and_save_to_csv(cursor_connection,customer_sql_query,customer_file_path)
    fetch_and_save_to_csv(cursor_connection,phishing_sql_query,phishing_file_path)

    #closing the connection
    cursor_connection.close()
    connection.close()
