# =====RAW SCHEMA========

raw = """"
CREATE SCHEMA IF NOT EXISTS {};
"""

# ======DEV SCHEMA =========
dev = """"
CREATE SCHEMA IF NOT EXISTS {};
"""

banks = """
CREATE TABLE IF NOT EXISTS {}.banks(
_id VARCHAR PRIMARY KEY NOT NULL,
code INTEGER NOT NULL,
name VARCHAR NOT NULL
);
"""
customers = """
CREATE TABLE IF NOT EXISTS {}.customers(
id INTEGER PRIMARY KEY NOT NULL,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
email VARCHAR NOT NULL,
registered_at DATE,
registered_year INTEGER,
registered_month INTEGER,
registered_day INTEGER,
registered_hour INTEGER

);
"""

exchange_rates = """
CREATE TABLE IF NOT EXISTS {}.exchange_rates(
date DATE NOT NULL,
bank_id VARCHAR NOT NULL,
rate NUMERIC
);
"""
items = """
CREATE TABLE IF NOT EXISTS {}.items(
id INTEGER PRIMARY KEY NOT NULL,
name VARCHAR NOT NULL,
selling_price NUMERIC,
cost_price NUMERIC
);
"""
transactions = """
CREATE TABLE IF NOT EXISTS {}.transactions(
id INTEGER PRIMARY KEY NOT NULL,
customer_id INTEGER NOT NULL,
item_id INTEGER NOT NULL,
bank_id VARCHAR NOT NULL,
transaction_date DATE,
transaction_year INTEGER,
transaction_month INTEGER,
transaction_day INTEGER
);
"""

raw_tables_queries = [banks, customers, exchange_rates, items, transactions]
