from pprint import pprint
import pytest
from queries.film2_queries.film_queries import *


def test_get_all_data(db_cursor):
    db_cursor.execute(get_all_data_query)
    result = db_cursor.fetchall()
    pprint(result)
    assert result is not None


def test_count_all_rows(db_cursor):
    db_cursor.execute(count_all_rows_query)
    result = db_cursor.fetchall()
    actual_result = result[0][0]
    assert actual_result == 1976


def test_get_movies_1940_1970(db_cursor):
    db_cursor.execute(get_movies_1940_1970_query)
    result = db_cursor.fetchall()
    for i in result:
        assert 1940 <= i[4] <= 1970


@pytest.mark.parametrize("writer", ['Eric von Stroheim', 'Kathryn Scola', 'Joe Eszterhas',
                                    'Lana Wachowski, David Mitchell, Aleksander Hemon'])
def test_movies_where_writer_is(db_cursor, writer):
    db_cursor.execute(movies_where_writer_is_query, (writer,))
    result = db_cursor.fetchall()
    for i in result:
        assert i[3] == writer
