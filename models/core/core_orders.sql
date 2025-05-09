{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with order_payments as (

    select
        order_id,

        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        {% endfor -%}

        sum(amount) as total_amount

    from {{ ref('stg_payments') }}

    group by order_id

)

select
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    orders.status,

    {% for payment_method in payment_methods -%}

    order_payments.{{ payment_method }}_amount,

    {% endfor -%}

    order_payments.total_amount as amount

from {{ ref('stg_orders') }} orders


left join order_payments
    on orders.order_id = order_payments.order_id
