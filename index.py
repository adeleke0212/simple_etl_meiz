from transformation import extract_transform_save_to_lake
from helper import (create_bucket, datawarehouse_connector,
                    RAW_SCHEMA, DEV_SCHEMA, s3_path, BUCKET_NAME, ARN_ROLE)
from sql.create import raw, raw_tables_queries, dev
from constants import db_tables
from sql.transform import dev_schema_create_tables
from sql.insert import insert_into_dev_schema

# Step 1: Create bucket in s3
try:
    create_bucket()
except Exception as e:
    print('Bucket already created')

# Step 2: Extract dataset from postgres, transform with pandas and save to lake

extract_transform_save_to_lake()

# Step 3: create schemas in warehouse


def create_schemas():
    print('Creating schema in warehouse')
    conn = datawarehouse_connector()
    cursor = conn.cursor()
    cursor.execute(raw.format(RAW_SCHEMA))
    cursor.execute(dev.format(DEV_SCHEMA))
    conn.commit()
    cursor.close()
    conn.close()
    print('schemas created')


# Creating raw tables in warehouse

def create_raw_schema_tables():
    conn = datawarehouse_connector()
    cursor = conn.cursor()
    for query in raw_tables_queries:
        print(f'Task: {query[:30]}')
        cursor.execute(query.format(RAW_SCHEMA))
        conn.commit()
    cursor.close()
    conn.close()
    print('Raw tables created in the warehouse')


# Step 4: copy from s3 to warehouse raw schema
# N.B delimiter  not delemeter
# Good intenet connection, hence time out error
def copy_from_s3_to_dwh():
    try:
        conn = datawarehouse_connector()
        cursor = conn.cursor()
        for table in db_tables:
            print(f"Copying {table} from s3 to dwh")
            copy_query = f"""
            copy {RAW_SCHEMA}.{table}
            from '{s3_path.format(BUCKET_NAME, table)}'
            iam_role '{ARN_ROLE}'
            delimiter ','
            ignoreheader 1;
            
            """
            cursor.execute(copy_query)
            conn.commit()
            print(f"{table} copied to dwh")
        cursor.close()
        conn.close()
    except Exception as e:
        print(e)

# Step 5: Create dev schema...Done
# Step 6: Create transformed dev schema tables


def create_dev_schema_tables():
    conn = datawarehouse_connector()
    cursor = conn.cursor()
    for query in dev_schema_create_tables:
        print(f"Task: {query[:35]}")
        cursor.execute(query.format(DEV_SCHEMA))
        conn.commit()
    print('All dev schema tables created in datawarehouse')
    cursor.close()
    conn.close()

# Step 7: Insert into transformed schema tables


def transformed_schema_insertions():
    conn = datawarehouse_connector()
    cursor = conn.cursor()
    for query in insert_into_dev_schema:
        print(f"Insert Task: {query[:30]}")
        cursor.execute(query.format(DEV_SCHEMA))
        conn.commit()
    print('All insert table jobs done')
    cursor.close()
    conn.close()
