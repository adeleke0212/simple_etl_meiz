from sqlalchemy import create_engine
from helper import database_connector
from helper import DATABASE, HOST, PASSWORD, USER, PORT, RAW_SCHEMA, DEV_SCHEMA
import os
import pandas as pd
from sql.create import dev, raw, raw_tables_queries
from sql.transform import dev_schema_create_tables
from sql.insert import insert_into_dev_schema


def alchemy_connection():
    conn = create_engine(
        f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    )
    return conn


def load_files_to_database():
    conn = alchemy_connection()
    db_conn = database_connector()
    cursor = db_conn.cursor()
    print('Creating raw schema in database')
    cursor.execute(raw.format(RAW_SCHEMA))
    for query in raw_tables_queries:
        print(f'Task: {query[:25]}')
        cursor.execute(query.format(RAW_SCHEMA))
        db_conn.commit()

    directory = 'transformed'
    for file in os.listdir(directory):
        file_name = file.split('.')[0]
        df = pd.read_csv(f'transformed/{file}')
        df.to_sql(file_name, con=conn, if_exists='replace',
                  schema=RAW_SCHEMA, index=False)
        print(f"file {file_name} loaded")
        db_conn.commit()
    cursor.close()
    db_conn.close()


def create_schema_jobs():
    conn = database_connector()
    cursor = conn.cursor()
    cursor.execute(dev.format(DEV_SCHEMA))
    print('Transformed schema created')
    for query in dev_schema_create_tables:
        print(f"Task: {query[:25]}")
        cursor.execute(query.format(DEV_SCHEMA))
        conn.commit()
    print('All dev tables Created')
    for query in insert_into_dev_schema:
        print(f"Task: {query[:25]}")
        cursor.execute(query.format(DEV_SCHEMA))
        conn.commit()
    print('All tables inserted')

    cursor.close()
    conn.close()
    print('All job done in database')


alchemy_connection()
load_files_to_database()
create_schema_jobs()
