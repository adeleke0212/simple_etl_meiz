dim_banks = """
INSERT INTO {}.dim_banks(
id,
bank_code,
bank_name,
exchange_rate,
exchange_rate_date
)
SELECT
b._id,
b.code,
b.name,
(e.rate :: NUMERIC) as exchange_rate,
(e.date :: date) as exchange_rate_date
FROM meiz_raw_schema.banks b
LEFT JOIN meiz_raw_schema.exchange_rates e
ON b._id = e.bank_id;
"""

dim_customers = """
INSERT INTO {}.dim_customers(
first_name,
last_name,
email,
registered_at,
registered_year,
registered_month,
registered_day,
registered_hour
)
SELECT
c.first_name,
c.last_name,
c.email,
(c.converted_date :: DATE),
c.registered_year,
c.registered_month,
c.registered_day,
c.registered_hour
FROM meiz_raw_schema.customers c
"""

dim_items = """
INSERT INTO {}.dim_items(
id,
truck_name,
cost_price,
selling_price
)
SELECT
i.id,
i.name,
i.cost_price,
i.selling_price
FROM meiz_raw_schema.items i
"""
ft_transactions = """
INSERT INTO {}.ft_transactions(
customer_id,
item_id,
bank_id,
truck_selling_price,
truck_cost_price_usd,
truck_qty,
total_trucks_cost,
total_trucks_sales,
exchange_rate,
transaction_date,
transaction_year,
transaction_month,
transaction_day
)
SELECT
c.id,
i.id,
b._id,
i.selling_price,
i.cost_price,
t.qty as truck_quantity,
(i.cost_price * t.qty) as total_trucks_cost,
(i.selling_price * t.qty) as total_trucks_sales,
e.rate as exchange_rate,
t.transaction_date :: DATE,
t.transaction_year,
t.transaction_month,
t.transaction_day
FROM meiz_raw_schema.transactions t
LEFT JOIN meiz_raw_schema.customers c
ON c.id = t.customer_id
LEFT JOIN meiz_raw_schema.banks b
ON t.bank_id = b._id
LEFT JOIN meiz_raw_schema.items i
ON
t.item_id = i.id
LEFT JOIN meiz_raw_schema.exchange_rates e
ON t.bank_id = e.bank_id
"""
insert_into_dev_schema = [dim_banks, dim_customers, dim_items, ft_transactions]
