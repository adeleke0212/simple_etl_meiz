dim_banks = """
CREATE TABLE IF NOT EXISTS {}.dim_banks(
id VARCHAR NOT NULL,
bank_code INTEGER NOT NULL,
bank_name VARCHAR NOT NULL,
exchange_rate NUMERIC,
exchange_rate_date DATE
);
"""
# Join exchange rate on bank_id
dim_customers = """
CREATE TABLE IF NOT EXISTS {}.dim_customers(
id BIGINT IDENTITY(1,1),
first_name VARCHAR(255),
last_name VARCHAR(255),
email varchar(255),
registered_at DATE,
registered_year INTEGER,
registered_month INTEGER,
registered_day INTEGER,
registered_hour INTEGER
);
"""

dim_items = """
CREATE TABLE IF NOT EXISTS {}.dim_items(
id INTEGER not null,
truck_name VARCHAR,
cost_price NUMERIC,
selling_price NUMERIC

);
"""
# ,
ft_transactions = """
CREATE TABLE IF NOT EXISTS {}.ft_transactions(
id BIGINT IDENTITY(1,1),
customer_id INTEGER NOT NULL,
item_id INTEGER NOT NULL,
bank_id VARCHAR NOT NULL,
truck_selling_price NUMERIC,
truck_cost_price_usd NUMERIC,
truck_qty INTEGER,
total_trucks_cost NUMERIC,
total_trucks_sales NUMERIC,
exchange_rate NUMERIC,
transaction_date DATE,
transaction_year INTEGER,
transaction_month INTEGER,
transaction_day INTEGER
);

"""
dev_schema_create_tables = [dim_banks,
                            dim_items, dim_customers, ft_transactions]
