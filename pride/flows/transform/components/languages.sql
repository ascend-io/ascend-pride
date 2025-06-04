SELECT
    lang.english AS language,
    COUNT(DISTINCT films.id) AS film_count
FROM {{ ref("read_films", flow="extract-load") }} AS films
INNER JOIN {{ ref("read_languages", flow="extract-load") }} AS lang
    ON films.original_language = lang.alpha2
GROUP BY lang.english
ORDER BY COUNT(DISTINCT films.id) DESC
