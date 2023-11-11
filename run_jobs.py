from index import (create_bucket, extract_transform_save_to_lake, create_schemas,
                   create_raw_schema_tables, copy_from_s3_to_dwh,
                   create_dev_schema_tables, transformed_schema_insertions)

try:
    create_bucket()
except Exception as e:
    print('Bucket already created')


def run_all_jobs():
    try:
        extract_transform_save_to_lake()
        create_schemas(),
        create_raw_schema_tables(),
        copy_from_s3_to_dwh()
        create_dev_schema_tables()
        transformed_schema_insertions()
        print('Entire pipeline completed successfully')
    except Exception as e:
        print(e)


run_all_jobs()
