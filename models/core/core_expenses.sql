
with customer_orders as (

    select
        customer_id,

        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from {{ ref('stg_orders') }}

    group by customer_id

),

customer_payments as (

    select
        orders.customer_id,
        sum(payments.amount) as total_amount

    from {{ ref('stg_payments' ) }} as payments

    left join {{ ref('stg_orders') }} as orders
        on payments.order_id = orders.order_id

    group by orders.customer_id

),

expenses as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value
    from {{ ref('stg_customers') }} as customers --comment here
    left join customer_orders  --comment here
      on customers.customer_id = customer_orders.customer_id  --comment here
    -- FROM customer_orders  --uncomment here
    -- left join staging.customers as customers  --uncomment here
    --     on  customer_orders.customer_id = customers.customer_id  --uncomment here
    left join customer_payments
        on customers.customer_id = customer_payments.customer_id
)

select * from expenses

