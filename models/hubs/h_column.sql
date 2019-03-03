select  {{ dbt_utils.surrogate_key('src.film_id') }} as film_hsh,
