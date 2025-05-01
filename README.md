# Jaffle shop example

This example is taken from the [`jaffle_shop` example](https://github.com/dbt-labs/jaffle_shop/) from dbt. Here is the scripts file structure:

```
models
├── analytics
│   ├── finance
│   │   └── kpis.sql
│   └── kpis.sql
├── core
│   ├── customers.sql
│   └── orders.sql.jinja
├── staging
│   ├── customers.sql
│   ├── orders.sql
│   └── payments.sql
└── tests
    └── orders_are_dated.sql
```

The first thing to do install environment using `uv` preferably

```sh
uv sync
```

This example uses DuckDB as the data warehouse.

You can run the models to populate tables:

```sh
uv run dbt run
```

There are a couple of cool things:

1. The orchestration is completely managed by `dbt`. E.g if `model_a` depends on `model_b`, it will be ran in correct order.
2. The staging schema is populated using SQL scripts and native DuckDB parsing of CSV files.
3. The `core.orders` table is created using a Jinja SQL script. `dbt` will automatically run the model through Jinja, and then execute the resulting SQL.
4. When doing a `dbt build`, the models will be built in fail fast. E.g tests are ran on each model, and if it fails, it will skip downstream models.

## Document

It's possible to generate the static site of documentation using :

```sh
uv run dbt docs generate
uv run dbt docs serve
```

and review the docs

![docs](img/docs.png)

- Tables are materialized since you ran earlier `dbt run`

## Write

- Add a new script `core/core_expenses.sql`

```sh
echo '''
with customer_orders as (

    select
        customer_id,

        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders
    from staging.orders

    group by customer_id

),

customer_payments as (

    select
        orders.customer_id,
        sum(payments.amount) as total_amount

    from staging.payments as payments

    left join staging.orders as orders
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
    from staging.customers as customers --comment here
    left join customer_orders  --comment here
      on customers.customer_id = customer_orders.customer_id  --comment here
    -- FROM customer_orders  --uncomment here
    -- left join staging.customers as customers  --uncomment here
    --     on  customer_orders.customer_id = customers.customer_id  --uncomment here
    left join customer_payments
        on customers.customer_id = customer_payments.customer_id
)

select * from expenses
''' > models/core/core_expenses.sql
```

```sh
echo '''
SELECT *
FROM {{ref('core_expenses')}}
WHERE customer_lifetime_value IS NULL

''' > tests/assert_customer_lifetime_value_not_null
```

## Audit

- Run the scripts `dbt build` : `lea_duckdb_max.tests.core__expenses__customer_lifetime_value___no_nulls___audit` is failing ❌
- Uncomment and comment lines to reverse the JOIN orders, and exclude customers absent from orders tables.

```sh
sed -i '' '/--comment here/s/^/--/' scripts/core/expenses.sql
sed -i '' '/--uncomment here/s/-- //' scripts/core/expenses.sql
```

- Run again scripts, you should see that all stagings audit tables are not executed again.
- `core.expenses` is executed as lea detected modification on the script
- All tests are now passing 🎉
- Audit tables are wiped out from development warehouse.

## Publish

- As all tests passed, tables are materialized in the development warehouse.
- If you want now to run it against production and not development warehouse, you would add a `--production` flag to each command:

```sh
lea run --production
```
