WITH films_with_century AS (
    SELECT
        *,
        CASE
            WHEN YEAR(release_date) BETWEEN 1900 AND 1999 THEN '20th'
            WHEN YEAR(release_date) BETWEEN 2000 AND 2025 THEN '21st'
            ELSE NULL
        END AS century
    FROM
        {{ ref('read_films', flow='extract-load') }}
    WHERE
        release_date IS NOT NULL
        AND YEAR(release_date) BETWEEN 1900 AND 2025
)

-- SELECT * FROM films_with_century LIMIT 10

SELECT
    title,
    overview,
    century,
    ROW_NUMBER() OVER (
        PARTITION BY century
        ORDER BY popularity DESC
    ) AS film_rank
FROM films_with_century
WHERE century IS NOT NULL
QUALIFY film_rank <= 10
ORDER BY century, film_rank


{{ with_test("not_null", column="title") }}
-- {{ with_test("not_null", column="popularity") }}
{{ with_test("less_than_or_equal", column="film_rank", value=10) }}
{{ with_test("count_greater_than", count=0) }}
