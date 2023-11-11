import boto3
import s3fs
import psycopg2
from configparser import ConfigParser
from sqlalchemy import create_engine
import pandas as pd
import redshift_connector

config = ConfigParser()

config.read('.env')

# Database connection credentials

HOST = config['DB_CONN']['host']
DATABASE = config['DB_CONN']['database']
USER = config['DB_CONN']['user']
PASSWORD = config['DB_CONN']['password']
PORT = config['DB_CONN']['port']

# s3 lake connection credentials
BUCKET_NAME = config['S3']['bucket_name']
ACCESS_KEY = config['S3']['access_key']
SECRET_KEY = config['S3']['secret_key']
REGION = config['S3']['region']

# Datawarehouse connection credentials

DWH_HOST = config['dwh_conn']['dwh_host']
DWH_USER = config['dwh_conn']['dwh_username']
DWH_DB = config['dwh_conn']['dwh_database']
DWH_PASSWORD = config['dwh_conn']['dwh_password']
ARN_ROLE = config['dwh_conn']['arn_role']

# Schema credentials
RAW_SCHEMA = config['dwh_conn']['raw_schema']
DEV_SCHEMA = config['dwh_conn']['dev_schema']


s3_path = 's3://{}/{}.csv'


def database_connector():
    conn = psycopg2.connect(
        host=HOST, user=USER, database=DATABASE, port=PORT, password=PASSWORD
    )
    print('Connected to database')
    return conn


def create_bucket():
    client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION

    )
    client.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION
        }
    )
    print('Bucket created')

# Creating connection to the database using alchemy


def sql_alchemy_db_conn():
    conn = create_engine(
        f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
    )
    print('Connected to Db using alchemy')
    return conn

# Saving datas to lake


def save_to_lake(s3_path, df):
    df.to_csv(s3_path, index=False, storage_options={
        'key': ACCESS_KEY, 'secret': SECRET_KEY
    })


def datawarehouse_connector():
    conn = redshift_connector.connect(
        host=DWH_HOST, database=DWH_DB, password=DWH_PASSWORD, user=DWH_USER
    )
    print('Connected to the database')
    return conn
