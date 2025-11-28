from config_loader import *
from util import *
import pandas as pd

def mysql_extract(table_name):
    query=f"select * from {table_name}"
    conn=connect_to_mysql()
    if conn is None:
        print("Error connecting to MySQL database")
        return None
    # print('Extracting data')
    df=pd.read_sql(query,conn)
    conn.close()
    return df

def sql_extract(table_name):
    query=f"select * from {table_name}"
    conn=connect_to_sql_server()
    if conn is None:
        print('Error connecting to SQL Server')
        return None
    df=pd.read_sql_query(query,conn)
    conn.close()
    return df

# # Testing---------------
# # sql dataframes
# sql_customer_df=sql_extract('CustomerData')
# sql_order_df=sql_extract('orders')
# sql_dial_codes_df=sql_extract('dial_codes')
#
# # mysql_dataframes
# mysql_customer_df=mysql_extract('customer')
# mysql_orders_df=mysql_extract('orders')
# mysql_dial_codes_df=mysql_extract('dial_codes')

