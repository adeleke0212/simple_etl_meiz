import pandas as pd
from constants import db_tables
from helper import sql_alchemy_db_conn, save_to_lake, s3_path, BUCKET_NAME
from sql.select import select_query


# Hour of day of the most registration
# Monthly and Quarterly gains and losses from transactions date


def extract_transform_save_to_lake():
    print('Extracting and Transforming datasets')
    conn = sql_alchemy_db_conn()
    for table in db_tables:
        if table == 'customers':
            df = pd.read_sql_query(select_query.format(table), con=conn)

            df['converted_date'] = df['registered_at'].astype(
                str).apply(lambda x: x[:10])
            df['converted_date'] = pd.to_datetime(df['converted_date'])

            df['registered_year'] = pd.DatetimeIndex(df['registered_at']).year
            df['registered_month'] = pd.DatetimeIndex(
                df['registered_at']).month
            df['registered_day'] = pd.DatetimeIndex(df['registered_at']).day

            df['registered_hour'] = df['registered_at'].astype(
                str).apply(lambda x: x[11:13]).astype(int)
            df['email'] = df['email'].fillna('Not provided')
            df[['first_name', 'last_name']] = df['name'].str.split(
                ' ', expand=True)
            df = df[['id', 'first_name', 'last_name', 'email', 'registered_at', 'converted_date',
                     'registered_year', 'registered_month', 'registered_day', 'registered_hour']]
            df.to_csv('transformed/customers.csv', index=False)
            save_to_lake(s3_path.format(BUCKET_NAME, table), df)
            print(f'extracted {table} and saved to lake')
        elif table == 'transactions':
            df = pd.read_sql_query(select_query.format(table), con=conn)
            df['date'] = pd.to_datetime(df['date'])
            df['transaction_year'] = df['date'].dt.year
            df['transaction_month'] = df['date'].dt.month
            df['transaction_day'] = df['date'].dt.day
            df = df.rename(columns={'date': 'transaction_date'})
            df = df[['id', 'customer_id', 'item_id', 'bank_id', 'qty', 'transaction_date', 'transaction_year', 'transaction_month',
                     'transaction_day']]
            df.to_csv('transformed/transactions.csv', index=False)
            save_to_lake(s3_path.format(BUCKET_NAME, table), df)
            print(f'extracted {table} and saved to lake')
        elif table == 'exchange_rates':
            df = df = pd.read_sql_query(select_query.format(table), con=conn)
            df['date'] = pd.to_datetime(df['date'])
            df.to_csv('transformed/exchange_rates.csv', index=False)
            save_to_lake(s3_path.format(BUCKET_NAME, table), df)
            print(f'extracted {table} and saved to lake')
        else:
            df = pd.read_sql_query(select_query.format(table), con=conn)
            df.to_csv(f'transformed/{table}.csv', index=False)
            save_to_lake(s3_path.format(BUCKET_NAME, table), df)
            print(f'extracted {table} and saved to lake')
    return df
