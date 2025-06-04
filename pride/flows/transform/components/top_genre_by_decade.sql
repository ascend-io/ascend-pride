SELECT
    *
FROM
    {{ ref("genres_by_decade") }}
WHERE genre_rank = 1
ORDER BY decade, genre_name

{{ with_test("not_null", column="decade") }}
{{ with_test("not_null", column="genre_rank") }}
{{ with_test("less_than_or_equal", column="genre_rank", value=1) }}
{{ with_test("count_greater_than", count=0) }}
