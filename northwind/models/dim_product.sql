with stg_products as (
    select * from {{ source('northwind', 'Products') }}
),
stg_categories as (
    select * from {{ source('northwind', 'Categories') }}
),
stg_suppliers as (
    select * from {{ source('northwind', 'Suppliers') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['stg_products.productid']) }} as productkey,
    stg_products.productid,
    stg_products.productname,
    stg_categories.categoryname,
    stg_suppliers.companyname as suppliername
from stg_products
left join stg_categories on stg_products.categoryid = stg_categories.categoryid
left join stg_suppliers on stg_products.supplierid = stg_suppliers.supplierid