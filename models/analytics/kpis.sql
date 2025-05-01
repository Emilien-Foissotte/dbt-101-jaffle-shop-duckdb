SELECT
    'n_customers' AS metric,
    COUNT(*) AS value
FROM
    {{ ref('core_customers') }}

UNION ALL

SELECT
    'n_orders' AS metric,
    COUNT(*) AS value
FROM
    {{ ref('core_orders') }}


UNION ALL

SELECT 
  'churn_percentage' AS metric,
      ROUND(
        100.0 * (
            SELECT COUNT(*)
            FROM {{ ref('core_churned_customers') }}
        ) / (
            SELECT COUNT(DISTINCT customer_id)
            FROM {{ ref('core_orders') }}
                        WHERE customer_id IN (
                SELECT customer_id
                FROM {{ ref('core_orders') }}
                GROUP BY customer_id
                HAVING COUNT(order_id) > 1
            )
        ),
        2
    ) AS value
