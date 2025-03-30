import pymysql
import os
import mysql.connector as mysql_connector
from dotenv import load_dotenv
#loading the current env file from working directory
load_dotenv()


#checking the MYSQL USER CREDENTIALS 
""" checking MYSQL CREDENTIALS """
MYSQL_USERNAME = os.getenv(key="MYSQL_USERNAME")  #fetching MYSQL USERNAME FROM ENVIRONMENT VARIABLE
MYSQL_PASSWORD = os.getenv(key="MYSQL_PASSWORD")  #fetching MYSQL PASSWORD FROM ENVIRONMENT VARIABLE
MYSQL_DATABASE = os.getenv(key="MYSQL_DATABASE")  #fetching MYSQL DATABASE FROM ENVIRONMENT VARIABLE
MYSQL_HOST = os.getenv(key="MYSQL_HOST")  #fetching MYSQL HOST  FROM ENVIRONMENT VARIABLE
MYSQL_PORT = os.getenv(key="MYSQL_PORT")  #fetching MYSQL PORT FROM ENVIRONMENT VARIABLE

#created an MYSQL Credentials as global context object 

""" Define test connection function to check the connection """
def test_connection(MYSQL_USERNAME,MYSQL_PASSWORD,MYSQL_DATABASE,MYSQL_HOST,MYSQL_PORT):
    """ checking the connection through provided credentials"""
    try:
        connection = pymysql.connect(
            host=MYSQL_HOST,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            user=MYSQL_USERNAME,
            port=int(MYSQL_PORT),
            autocommit=True
        )
        if connection:
            print("Connection test successfull")
        else:
            print("Connection not established")
    except pymysql.Error as error:
        raise error    #catching the exception error from pymysql module
    except pymysql.DatabaseError as database_error:
        raise database_error  #catching the exception related to database error
    except pymysql.InterfaceError as interface_error:
        raise interface_error  #catching the exception related to database interface error
    except pymysql.InternalError as internal_error:
        raise internal_error   #catching the exception related to database internal system error


""" Calling the Test connection function to check the connection for database"""    
test_connection(MYSQL_USERNAME,MYSQL_PASSWORD,MYSQL_DATABASE,MYSQL_HOST,MYSQL_PORT)