WITH customer_orders AS (

        SELECT
        customer_id,
        MIN(order_date) as first_order,
        MAX(order_date) as most_recent_order,
        COUNT(order_id) as number_of_orders

    from  {{ ref('stg_orders') }}

    group by customer_id

),

customer_payments AS (

    SELECT
        orders.customer_id,
        SUM(amount) AS total_amount,
        MEDIAN(amount) AS median_order_value,
        AVG(amount) AS average_order_value


    FROM {{ ref('stg_payments') }} AS payments

    LEFT JOIN {{ ref('stg_orders') }} AS orders USING (order_id)

    GROUP BY orders.customer_id

)

SELECT
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customer_orders.first_order,
    customer_orders.most_recent_order,
    customer_orders.number_of_orders,
    customer_payments.average_order_value,
    customer_payments.median_order_value,
    customer_payments.total_amount as customer_lifetime_value

FROM {{ ref('stg_customers') }} AS customers

LEFT JOIN customer_orders
    ON customers.customer_id = customer_orders.customer_id

LEFT JOIN customer_payments
    ON  customers.customer_id = customer_payments.customer_id
