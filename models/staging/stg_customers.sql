WITH raw_customers AS (
    SELECT * FROM 'seeds/raw_customers.csv'
)

SELECT
    id AS customer_id,
    first_name,
    last_name
FROM raw_customers
