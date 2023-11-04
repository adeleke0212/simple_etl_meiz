from transformation import extract_transform_save_to_lake
from helper import create_bucket, datawarehouse_connector, RAW_SCHEMA, DEV_SCHEMA
from sql.create import raw, raw_tables_queries, dev

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
