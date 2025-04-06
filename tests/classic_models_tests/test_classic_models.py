from decimal import Decimal
from pprint import pprint
import pytest
from queries.classic_model_quires.classic_models_queries import *


def test_get_top_vendor(db_cursor):
    """Test the get_top_vendor function using a fixture."""
    db_cursor.execute(get_top_vendor_query)
    result = db_cursor.fetchall()
    print(result)
    assert result is not None


@pytest.mark.parametrize("country", ["USA", "France", "Germany"])
def test_fetch_customers_by_country(db_cursor, country):
    db_cursor.execute(fetch_customers_by_country_query, (country,))
    result = db_cursor.fetchall()
    pprint(result, indent=4)
    assert any(country in row[2] for row in result)


def test_get_vendors_product_lines_products(db_cursor):
    db_cursor.execute(vendors_product_lines_products_query)
    result = db_cursor.fetchone()
    print(result)
    assert result == (110, 7, 13)


def test_get_avg_msrp_buy_price(db_cursor):
    db_cursor.execute(avg_msrp_buy_price_query)
    result = db_cursor.fetchall()

    pprint(result, indent=4)
    print(result[0])
    print(f"Result returned {len(result)} rows")
    assert result[0]['productVendor'].__contains__('Welly Diecast Productions')
    assert len(result) == 13


def test_max_sails_vendor(db_cursor):
    db_cursor.execute(max_sails_vendor_query)
    result = db_cursor.fetchone()
    print(result)
    from decimal import Decimal
    assert result == ({'productVendor': 'Classic Metal Creations', 'total_orders': Decimal('9678')})


def test_most_sold_product(db_cursor):
    db_cursor.execute(most_sold_product_query)
    result = db_cursor.fetchone()
    print(result)
    assert result == ('1992 Ferrari 360 Spider red', Decimal('1808'))


def test_buyprice_msrp_difference(db_cursor):
    db_cursor.execute(buyprice_msrp_difference_query)
    result = db_cursor.fetchall()

    pprint(result, indent=4)
    assert result[0] == ('18th century schooner',
                         Decimal('82.34'),
                         Decimal('122.89'),
                         Decimal('-40.55'))


def test_get_union_buy_price(db_cursor):
    db_cursor.execute(get_union_buyprice_query)
    result = db_cursor.fetchall()
    print(result)
    assert len(result) == 110


def test_get_all_canceled_orders(db_cursor):
    db_cursor.execute(get_all_canceled_orders_query)
    result = db_cursor.fetchone()
    assert result[0] == 6


def test_all_orders_created(db_cursor):
    db_cursor.execute(all_orders_created_query)
    result = db_cursor.fetchone()
    assert result[0] == 326


def test_number_of_products_per_product_line(db_cursor):
    expected_list = [('Classic Cars', 38), ('Motorcycles', 13), ('Planes', 12), ('Ships', 9), ('Trains', 3),
                     ('Trucks and Buses', 11), ('Vintage Cars', 24)]
    db_cursor.execute(number_of_products_per_product_line_query)
    result = db_cursor.fetchall()
    assert result == expected_list


def test_number_of_no_customer_employees(db_cursor):
    db_cursor.execute(number_of_no_customer_employees_query)
    result = db_cursor.fetchone()
    assert result[0] == 8
