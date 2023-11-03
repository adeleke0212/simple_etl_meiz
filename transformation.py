import pandas as pd
from constants import db_tables
from helper import sql_alchemy_db_conn
from sql.select import select_query


def extract_and_transform_tables():
    print('Extracting and Transforming customers data')
    for table in db_tables:
        if table == 'customers':
            conn = sql_alchemy_db_conn()
            df = pd.read_sql_query(select_query.format(table), con=conn)
            df['registered_at'] = pd.to_datetime(df['registered_at'])
            df['registered_year'] = df['registered_at'].dt.year
            df['registered_month'] = df['registered_at'].dt.month
            df['registered_day'] = df['registered_at'].dt.day
            # df['registered_hour'] = df['registered_at'].dt.hour
            df['registered_hour'] = df['registered_at'].astype(str).apply(lambda x: x[11:13]).astype(int)
            df['email'] = df['email'].fillna('Not provided')
            df[['first_name', 'last_name']] = df['name'].str.split(' ', expand=True)
            df = df[['id', 'first_name', 'last_name', 'email', 'registered_at', 'registered_year', 'registered_month', 'registered_day', 'registered_hour']]
            df.to_csv('transformed/customers.csv', index= False)
        elif table == 'transactions':
            




