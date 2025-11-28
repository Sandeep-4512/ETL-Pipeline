from config_loader import *
import pyodbc #type:ignore
import mysql.connector #type:ignore
from mysql.connector import Error #type:ignore
config=load_config()
sql_conn=None
mysql_conn=None

# SQL Server credentials

def connect_to_sql_server():
    sql_server = config['sql server']['server']
    sql_database = config['sql server']['database1']
    driver = config['sql server']['driver']
    try:
        conn=pyodbc.connect(f"""
        Driver={{{driver}}};
        SERVER={sql_server};
        Trusted_Connection=Yes;
        database={sql_database};
    """
        )
        # print(f"Connected to SQL Server: {sql_server},Database: {sql_database}")
        return conn
    except Exception as e:
        print(f"Failed to connect to SQL Server: {e}")
        return None

def connect_to_mysql():
    host=config['my sql']['host']
    user=config['my sql']['user']
    password=config['my sql']['password']
    database=config['my sql']['database']
    try:
        conn=mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        # print(f"Connected to MySQL Server: {user},Database: {database}")
        return conn
    except Exception as e:
        print(f"Failed to connect to MySQL Server: {e}")
        return None

# # Testing-----------------------------
# # MySQL
# try:
#     connect_to_mysql()
#     print('Connected to MySQl')
# except Exception as e:
#     print(f"Error connecting to MySQL:{e}")
# # SQL Server
# try:
#     connect_to_sql_server()
#     print('Connected to SQL Server')
# except Exception as e:
#     print(f'Error connecting to SQL Server:{e}')

