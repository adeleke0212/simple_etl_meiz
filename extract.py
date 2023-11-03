from constants import db_tables
import pandas as pd
import os
from helper import database_connector, create_bucket, sql_alchemy_db_conn
from sql.copy import copy_query

# seems read_sql_query works well with sqlalchemyconnection rather than psycopg2
# from the error pandas only supports SQLAlchemy connection


def extract_from_postgres():
    conn = sql_alchemy_db_conn()
    for table in db_tables:
        df = pd.read_sql_query(copy_query.format(table), con=conn)
        df.to_csv(f'Datasets/{table}.csv', index=False)


extract_from_postgres()
