from extract import *
import pandas as pd
import numpy as np

def transform_orders(dataframe):
    dataframe=dataframe.drop_duplicates()
    dataframe=dataframe.dropna()
    dataframe['order_date']=pd.to_datetime(dataframe['order_date'],format='mixed',errors='coerce').dt.date
    dataframe['order_amount']=pd.to_numeric(dataframe['order_amount'],errors='coerce')
    dataframe = dataframe.replace({pd.NaT: None, np.nan: None})
    return dataframe

def transform_customers(dataframe,dial_codes_df):
    dataframe=dataframe.drop_duplicates()
    dataframe['email_flag']=dataframe['email'].astype(str).str.match(r'^[a-zA-Z0-9_.]+@[a-z]+\.[a-z]{2,}$',na=False)
    dataframe['phone_flag']=dataframe['phone'].astype(str).str.match(r'^\d{3}-\d{3}-\d{4}$',na=False)
    dataframe['email']=dataframe.apply(lambda row:f"{row['name'].lower().strip().replace(' ','_')}@email.com" if pd.isnull(row['email']) or row['email_flag']==False else row['email'],axis=1)
    dataframe['phone']=dataframe.apply(lambda row:"000-000-0000" if pd.isnull(row['phone']) or row['phone_flag']==False else row['phone'],axis=1)
    dataframe.drop(columns=['email_flag','phone_flag'],inplace=True)
    dataframe['registration_date']=pd.to_datetime(dataframe['registration_date'],format='mixed',errors='coerce').dt.date
    dataframe['name']=dataframe['name'].astype(str).str.strip().str.replace(r'Mr\.|Mrs\.|Jr\.|Sr\.|Dr\.|I|II|III|Miss\.','',regex=True)
    dataframe[['first_name','last_name']]=dataframe['name'].astype(str).str.split(' ',n=1,expand=True)
    dataframe.drop(columns=['name'],inplace=True)
    dataframe['state_code']=dataframe['address'].astype(str).str[-8:-6]
    dataframe['country_code']='US'
    # dial_codes_df=dial_codes_df[['A2','DIALINGCODE']]
    # dial_codes_df['country_code']=dial_codes_df['A2']
    # dataframe=dataframe.merge(dial_codes_df,on='country_code',how='left')
    dial_dict=dict(zip(dial_codes_df['A2'],dial_codes_df['DIALINGCODE']))
    dataframe['dial_code']='+'+dataframe['country_code'].map(dial_dict)+'-'
    dataframe['phone']=dataframe['dial_code']+dataframe['phone']
    dataframe.drop(columns=['dial_code'],inplace=True)
    loyalty_map_dict={
        'Gold':2,
        'Silver':1,
        'Bronze':0
    }
    dataframe['loyalty_category']=dataframe['loyalty_status'].map(loyalty_map_dict)
    dataframe = dataframe.replace({pd.NaT: None, np.nan: None})
    return dataframe

def unified_customers_orders_data(customer_df,orders_df):
    dataframe=customer_df.merge(orders_df,on='customer_id',how='left')
    dataframe = dataframe.replace({pd.NaT: None, np.nan: None})
    return dataframe

# # Testing---------------------------------------
# # sql tables
# sql_clean_customers_df=transform_customers(sql_customer_df,sql_dial_codes_df)
# sql_clean_orders_df=transform_orders(sql_order_df)
# sql_unified_df=unified_customers_orders_data(sql_clean_customers_df,sql_clean_orders_df)
#
# # mysql tables
# mysql_clean_customers_df=transform_customers(mysql_customer_df,mysql_dial_codes_df)
# mysql_clean_orders_df=transform_orders(mysql_orders_df)
# mysql_unified_df=unified_customers_orders_data(mysql_clean_customers_df,mysql_clean_orders_df)