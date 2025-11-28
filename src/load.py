from transform import *
from util import  *

def sql_create_table_and_load_data(table_name,dataframe):
    conn=connect_to_sql_server()
    if conn is None:
        print('Unable to connect to SQL server.Table creation failed')
        return False
    print('SQL Server load operation')
    cursor = conn.cursor()
    try:
        query=f"""create table {table_name}(
        id int identity(1,1) primary key,
        customer_id int,
        first_name varchar(50),
        last_name varchar(50),
        email varchar(50),
        phone varchar(50),
        address varchar(255),
        registration_date date,
        loyalty_status varchar(25),
        loyalty_category int,
        state_code varchar(25),
        country_code varchar(25),
        order_id varchar(255),
        order_date date,
        order_amount decimal(10,2),
        order_status varchar(25),
        product_category varchar(50)
        )"""
        cursor.execute(query)
        print(f"Table {table_name} created.")
    except Exception as e:
        print(f'Error creating table {table_name}:{e}')
        return False
    try:
        query=f"""insert into {table_name}(
        customer_id,first_name,last_name,email,phone,address,registration_date,loyalty_status,loyalty_category,state_code,country_code,
        order_id,order_date,order_amount,order_status,product_category
        )
        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        for row in dataframe.itertuples():
            cursor.execute(query,
                           (row.customer_id,
                           row.first_name,
                           row.last_name,
                           row.email,
                           row.phone,
                           row.address,
                           row.registration_date,
                           row.loyalty_status,
                           row.loyalty_category,
                           row.state_code,
                           row.country_code,
                           row.order_id,
                           row.order_date,
                           row.order_amount,
                           row.order_status,
                           row.product_category))
        conn.commit()
        print(f"Inserted {len(dataframe)} rows into {table_name}")
        conn.commit()
    except Exception as e:
        print(f"Error inserting data:{e}\nTable creation reverted.")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
#     end

def mysql_create_table_and_insert_data(table_name,dataframe):
    conn=connect_to_mysql()
    if conn is None:
        print("Error connecting to MySQL")
        return False
    print('MySQL Load Operation')
    cursor=conn.cursor()
    try:
        query = f"""create table {table_name}(
                id int AUTO_INCREMENT primary key,
                customer_id int,
                first_name varchar(50),
                last_name varchar(50),
                email varchar(50),
                phone varchar(50),
                address varchar(255),
                registration_date date,
                loyalty_status varchar(25),
                loyalty_category int,
                state_code varchar(25),
                country_code varchar(25),
                order_id varchar(255),
                order_date date,
                order_amount float,
                order_status varchar(25),
                product_category varchar(50)
                )"""
        cursor.execute(query)
        print(f"Table {table_name} created.")
    except Exception as e:
        print(f'Error creating table {table_name}:{e}')
        return False
    try:
        query = f"""insert into {table_name}(
                customer_id,first_name,last_name,email,phone,address,registration_date,loyalty_status,loyalty_category,state_code,country_code,
                order_id,order_date,order_amount,order_status,product_category
                )
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        for _,row in dataframe.iterrows():
            cursor.execute(query,
                           (row.customer_id,
                           row.first_name,
                           row.last_name,
                           row.email,
                           row.phone,
                           row.address,
                           row.registration_date,
                           row.loyalty_status,
                           row.loyalty_category,
                           row.state_code,
                           row.country_code,
                           row.order_id,
                           row.order_date,
                           row.order_amount,
                           row.order_status,
                           row.product_category))
        conn.commit()
        print(f"Inserted {len(dataframe)} rows into {table_name}")
        conn.commit()
    except Exception as e:
        print(f"Error inserting data:{e}\nTable creation reverted.")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# # Testing ------------------------------------------------------------------
# sql_create_table_and_load_data(table_name='clean_data',dataframe=mysql_unified_df)
# mysql_create_table_and_insert_data(table_name='clean_data',dataframe=mysql_unified_df)









