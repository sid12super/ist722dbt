with stg_orders as (
    select 
        OrderId,
        CustomerId,
        EmployeeId,
        OrderDate
    from {{ source('northwind', 'Orders') }}
),
stg_order_details as (
    select 
        OrderId,
        ProductId,
        Quantity,
        UnitPrice,
        Discount
    from {{ source('northwind', 'Order_Details') }}
)
select
    -- Generate a unique surrogate key for the fact table
    {{ dbt_utils.generate_surrogate_key(['stg_order_details.OrderId', 'stg_order_details.ProductId']) }} as saleskey,
    stg_orders.OrderId,
    stg_orders.CustomerId,
    stg_orders.EmployeeId,
    stg_orders.OrderDate,
    stg_order_details.ProductId,
    stg_order_details.Quantity,
    stg_order_details.UnitPrice,
    -- Calculations based on your requirements
    (stg_order_details.Quantity * stg_order_details.UnitPrice) as ExtendedPrice,
    (stg_order_details.Quantity * stg_order_details.UnitPrice * stg_order_details.Discount) as DiscountAmount,
    (stg_order_details.Quantity * stg_order_details.UnitPrice * (1 - stg_order_details.Discount)) as SoldAmount
from stg_order_details
join stg_orders on stg_order_details.OrderId = stg_orders.OrderId