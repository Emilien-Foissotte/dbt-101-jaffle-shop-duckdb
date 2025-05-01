SELECT *
FROM {{ref('core_orders')}}
WHERE order_date IS NULL
