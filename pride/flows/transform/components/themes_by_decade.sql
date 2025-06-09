-- Step 1: Assign decade and extract themes for each film
WITH films_with_decade AS (
    SELECT
        *,
        FLOOR(YEAR(RELEASE_DATE) / 10) * 10 AS decade
    FROM {{ ref("read_films", flow="extract-load") }}
    WHERE RELEASE_DATE IS NOT NULL
),

film_themes AS (
    SELECT
        decade,
        TITLE,
        SNOWFLAKE.CORTEX.SUMMARIZE(
            'Summarize the main themes of this movie based on its overview: ' || OVERVIEW
        ) AS film_themes
    FROM films_with_decade
    WHERE OVERVIEW IS NOT NULL
),

-- Step 2: Aggregate a limited number of film themes for each decade
decade_theme_agg AS (
    SELECT
        decade,
        LISTAGG(film_themes, '; ') WITHIN GROUP (ORDER BY TITLE) AS all_film_themes,
        COUNT(*) AS num_films
    FROM (
        SELECT
            decade,
            TITLE,
            film_themes
        FROM film_themes
        QUALIFY ROW_NUMBER() OVER (PARTITION BY decade ORDER BY RANDOM()) <= 100  -- Limit to 100 films per decade
    )
    GROUP BY decade
)

SELECT
    decade,
    SNOWFLAKE.CORTEX.SUMMARIZE(
        LEFT(all_film_themes, 9000)  -- Let CORTEX summarize without complex instructions
    ) AS decade_themes_summary,
    num_films
FROM decade_theme_agg

{{ with_test("not_null", column="decade") }}
{{ with_test("not_null", column="decade_themes_summary") }}
