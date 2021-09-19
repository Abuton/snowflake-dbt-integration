with source_sales_store as (
    select * from {{ source('sample_data', 'customer') }}
),

final as (
    select * from source_sales_store
)

SELECT * FROM final

-- https://ocdsdeploy.readthedocs.io/en/latest/deploy/redash.html
-- superset run -p 8088 --with-threads --reload --debugger
-- https://superset.apache.org/docs/installation/installing-superset-from-scratch
