
get_all_data_query = """
    select * 
        from film2.film_locations_in_san_francisco
    limit 10;
    """


count_all_rows_query = """
    select count(*) 
        from film2.film_locations_in_san_francisco;
        """


get_movies_1940_1970_query = """
    select distinct(Title), `Production Company`, 
        Director, `Actor 1`, `Release Year`, `Fun Facts`
        from film_locations_in_san_francisco
        where `Release Year` between 1940 and 1970
    order by `Release Year` desc;
            """


movies_where_writer_is_query = """
    SELECT distinct(Title), `Release Year`, Director, Writer, `Actor 1`
        FROM `film2`.`film_locations_in_san_francisco`
    where Writer = %s;
    """
