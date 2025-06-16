CREATE SCHEMA raw_cdc;
SET search_path TO raw_cdc;

drop table if exists postgres.raw_cdc.customers;
drop table if exists postgres.raw_cdc.merchants;
drop table if exists postgres.raw_cdc.products;
drop table if exists postgres.raw_cdc.transactions;

CREATE TABLE postgres.raw_cdc.customers (
    customer_id integer PRIMARY KEY,
    firstname varchar,
    lastname varchar,
    age integer,
    email varchar,
    phone_number varchar
);

create table postgres.raw_cdc.merchants (
    merchant_id integer PRIMARY KEY,
	merchant_name varchar,
	merchant_category varchar
);

create table postgres.raw_cdc.products (
    product_id integer PRIMARY KEY,
    product_name varchar,
    product_category varchar,
    price double precision
);

create table postgres.raw_cdc.transactions (
    transaction_id varchar PRIMARY KEY,
	customer_id integer,
	product_id integer,
	merchant_id integer,
	transaction_date date,
	transaction_time varchar,
	quantity integer,
	total_price double precision,
    transaction_card varchar,
    transaction_category varchar
);

copy postgres.raw_cdc.customers from '/tmp/customers.csv' DELIMITER ',' CSV HEADER;
copy postgres.raw_cdc.merchants from '/tmp/merchants.csv' DELIMITER ',' CSV HEADER;
copy postgres.raw_cdc.products from '/tmp/products.csv' DELIMITER ',' CSV HEADER;
copy postgres.raw_cdc.transactions from '/tmp/transactions.csv' DELIMITER ',' CSV HEADER;


-- The publication is required to start the replication progress as the Connector is based on PostgreSQL Logical Replication
CREATE PUBLICATION agent_postgres_publication FOR ALL TABLES;

select * from postgres.raw_cdc.customers;
select * from postgres.raw_cdc.merchants;
select * from postgres.raw_cdc.products;
select * from postgres.raw_cdc.transactions;

CREATE OR REPLACE PROCEDURE insert_transactions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_new_transaction_id TEXT;
    v_customer_id INT;
    v_product_id INT;
    v_merchant_id INT;
    v_transaction_date DATE;
    v_transaction_time TEXT;
    v_quantity INT;
    v_product_price DOUBLE PRECISION;
    v_total_price DOUBLE PRECISION;
    v_existing_customer RECORD;
    v_existing_product RECORD;
    v_existing_merchant RECORD;
    v_transaction_card TEXT;
    v_transaction_category TEXT;
BEGIN
    -- Loop for 30 minutes (inserting 1000 records every minute)
    FOR i IN 1..30 LOOP
        FOR j IN 1..100 LOOP
            -- Select random valid customer, product, and merchant from existing tables
            SELECT * INTO v_existing_customer
            FROM postgres.raw_cdc.customers
            ORDER BY RANDOM()
            LIMIT 1;

            SELECT * INTO v_existing_product
            FROM postgres.raw_cdc.products
            ORDER BY RANDOM()
            LIMIT 1;

            SELECT * INTO v_existing_merchant
            FROM postgres.raw_cdc.merchants
            ORDER BY RANDOM()
            LIMIT 1;

            -- Generate new transaction ID (unique)
            v_new_transaction_id := 'TX' || EXTRACT(EPOCH FROM NOW())::TEXT || j::TEXT;

            -- Generate current date and time
            v_transaction_date := CURRENT_DATE;
            v_transaction_time := TO_CHAR(NOW(), 'HH24:MI:SS');

            -- Generate random quantity between 1 and 7
            v_quantity := FLOOR(RANDOM() * 7 + 1);

            -- Get product price and calculate total price
            v_product_price := v_existing_product.price;
            v_total_price := v_product_price * v_quantity;

            v_transaction_card := (ARRAY['American Express', 'Visa', 'Mastercard', 'Discover'])[FLOOR(RANDOM() * 4 + 1)];
            v_transaction_category := CASE WHEN RANDOM() < 0.8 THEN 'Purchase' ELSE 'Refund' END;

            -- Insert new transaction into the transactions table
            INSERT INTO postgres.raw_cdc.transactions (
                transaction_id, customer_id, product_id, merchant_id, transaction_date, transaction_time, quantity, total_price, transaction_card, transaction_category
            )
            VALUES (
                v_new_transaction_id, v_existing_customer.customer_id, v_existing_product.product_id,
                v_existing_merchant.merchant_id, v_transaction_date, v_transaction_time,
                v_quantity, v_total_price, v_transaction_card, v_transaction_category
            );
        END LOOP;

        -- Commit after every batch of 1000 rows
        COMMIT;

        -- Wait for 30 seconds before inserting the next batch
        PERFORM pg_sleep(30);
    END LOOP;
END;
$$;

CALL insert_transactions();
