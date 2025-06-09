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

-- -- Step 3: Summarize the most common themes for each decade
-- SELECT
--     decade,
--     SNOWFLAKE.CORTEX.SUMMARIZE(
--         'Given the following list of movie themes from the ' || decade || 's, summarize the most common themes of this decade: ' ||
--         LEFT(all_film_themes, 10000) || 
--         'Reference specific films only in parenthesis and follow this format: The 1950s saw a range of movie themes, including technology and rebellion ("The Mechanical Brain"), exploration of sexuality and identity ("The Trans Woman" and "Olivia"), housewifery and relationships ("The Inexperienced Housewife"), and moral dilemmas and crises of faith ("The Vicar of Bellington" and "A Double Life"). Other themes included communication between prisoners, cover-ups, and escapes.'
--     ) AS decade_themes_summary,
--     num_films
-- FROM decade_theme_agg
-- ORDER BY decade

{{ with_test("not_null", column="decade") }}
{{ with_test("not_null", column="decade_themes_summary") }}