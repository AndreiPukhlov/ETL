

get_top_vendor_query = """
    SELECT pr.productVendor, SUM(od.quantityOrdered) AS total_orders
        FROM products pr
        JOIN orderdetails od ON pr.productCode = od.productCode
        GROUP BY pr.productVendor
        ORDER BY total_orders DESC
    LIMIT 1;
    """

fetch_customers_by_country_query = "SELECT customerName, city, country FROM customers WHERE country = %s"

vendors_product_lines_products_query = """
    select count(distinct productCode) as products_total,
        count(distinct productLine) as prodlines_total,
        count(distinct productVendor) as vendors_total
    from products;
        """

avg_msrp_buy_price_query = """
    select productVendor, avg(buyPrice) as avg_buy_price,
        avg(MSRP) as avg_msrp
        from products
        group by productVendor
    order by avg_buy_price desc;
        """

max_sails_vendor_query = """
    select pr.productVendor, sum(od.quantityOrdered) as total_orders
        from products pr
        join orderdetails od on pr.productCode=od.productCode
        group by pr.productVendor
        order by total_orders desc
    limit 1;
        """

most_sold_product_query = """
    select pr.productName, sum(od.quantityOrdered) as total_sold
        from products as pr
        join orderdetails as od
        on pr.productCode=od.productCode
        group by pr.productName
        order by total_sold desc
    limit 1;
        """

buyprice_msrp_difference_query = """
   SELECT SUM(prod.msrp * det.quantityOrdered) as msrp_sales,
SUM(prod.buyPrice * det.quantityOrdered) as buyPrice_sales,
SUM(prod.msrp * det.quantityOrdered) - SUM(prod.buyPrice * det.quantityOrdered) as difference_in_sales
FROM classicmodels.products prod
JOIN classicmodels.orderdetails det on prod.productCode = det.productCode;
            """

get_union_buyprice_query = """
    select productName, buyPrice from products
        where buyPrice > 100
    union
        select productName, buyPrice from products
        where buyPrice < 200
    order by buyPrice desc;
    """


get_all_canceled_orders_query = """
    select count(*)
        from (select o.orderDate, o.shippedDate, o.comments, o.status
        customerName
        from orders as o
        join customers as c on o.customerNumber=c.customerNumber
    where status = 'Cancelled') as total_canceled;
    """


all_orders_created_query = """
    select count(*)
        from (select o.orderDate, o.shippedDate, o.comments, o.status
        customerName
        from customers as c
    join orders as o on c.customerNumber=o.customerNumber) as all_orders;
    """

number_of_products_per_product_line_query = """
    select productLine, count(distinct productName) as orig_products_per_product_line
        from products
    group by productLine;
    """

number_of_no_customer_employees_query = """
    select count(*) from
        (select concat(firstName, "", lastName) as saler_name,
        count(distinct customerName) as customer
        from employees as e
        left join customers as c on e.employeeNumber=c.salesRepEmployeeNumber
        group by saler_name
    having customer = 0) as no_customer_emloyee;
    """

