with f_sales as (
    select * from {{ ref('fact_sales') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_employee as (
    select * from {{ ref('dim_employee') }}
),
d_product as (
    select * from {{ ref('dim_product') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
)
select
    -- Dimension Columns (add specific columns you want from each dim)
    d_customer.*,
    d_employee.*,
    d_product.*,
    d_date.*,
    -- Fact Columns
    f.OrderID,
    f.Quantity,
    f.UnitPrice,
    f.ExtendedPrice,
    f.DiscountAmount,
    f.SoldAmount
from f_sales as f
left join d_customer on f.customerid = d_customer.customerid
left join d_employee on f.employeeid = d_employee.employeeid
left join d_product on f.productid = d_product.productid
left join d_date on f.orderdate = d_date.date