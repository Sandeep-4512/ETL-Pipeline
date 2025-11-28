from config_loader import *
from util import connect_to_mysql,connect_to_sql_server
from extract import mysql_extract,sql_extract
from transform import transform_customers,transform_orders,unified_customers_orders_data
from load import mysql_create_table_and_insert_data,sql_create_table_and_load_data
from util import *

def main():
    # print(mysql_customer_df,'\n',mysql_orders_df,'\n',sql_customer_df,'\n',sql_order_df)
    # print(sql_clean_orders_df,'\n',sql_clean_customers_df,'\n',mysql_clean_orders_df,'\n',mysql_clean_customers_df)
    # print(sql_unified_df,'\n',mysql_unified_df)
    # sql_unified_df.to_csv('sql_unified_data.csv',index=False)
    # sql_create_table_and_load_data(table_name='sql_unified_df',dataframe=sql_unified_df)
    # mysql_create_table_and_insert_data(table_name='mysql_unified_df',dataframe=mysql_unified_df)
    pass
if __name__ == '__main__':
    main()
