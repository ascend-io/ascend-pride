-- Find the most popular genre names by decade

WITH films_with_decade AS (
    SELECT
        *,
        FLOOR(YEAR(RELEASE_DATE) / 10) * 10 AS decade
    FROM {{ ref("map_genres") }}
    WHERE RELEASE_DATE IS NOT NULL
),

genre_counts AS (
    SELECT
    decade,
    g.value::STRING AS genre_name,
    COUNT(DISTINCT ORIGINAL_TITLE) AS film_count
    FROM films_with_decade,
        LATERAL FLATTEN(input => genre_name) g
    WHERE g.value IS NOT NULL
    GROUP BY decade, g.value::STRING
),

ranked_genres AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY decade ORDER BY film_count DESC) AS genre_rank
    FROM genre_counts
)

SELECT *
FROM ranked_genres
ORDER BY decade, genre_rank, genre_name

{{ with_test("not_null", column="decade") }}
{{ with_test("not_null", column="genre_name") }}
{{ with_test("count_greater_than", count=0) }}