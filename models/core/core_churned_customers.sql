WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders,
        MEDIAN(amount) AS median_order_value,
        MAX(order_date) AS last_order_date
    FROM {{ ref('core_orders') }}
    GROUP BY customer_id
),
max_order_date AS (
    SELECT MAX(order_date) AS max_date
    FROM {{ ref('core_orders') }}
),
churned_customers AS (
    SELECT
        customer_id,
        median_order_value,
        DATE_DIFF('day', last_order_date, max_date) AS days_since_last_order
    FROM customer_orders, max_order_date
    WHERE total_orders > 1
      AND DATE_DIFF('day', last_order_date, max_date) > 30
)
SELECT
    customer_id,
    median_order_value,
    ROUND(median_order_value * 0.20, 2) AS coupon_amount
FROM churned_customers

