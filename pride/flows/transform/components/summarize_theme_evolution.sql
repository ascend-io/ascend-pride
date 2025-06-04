SELECT
    SNOWFLAKE.CORTEX.SUMMARIZE(
        'Given the following summaries of movie themes by decade, describe how movie themes have evolved over time. Highlight major shifts, recurring motifs, and any notable trends. Summaries: ' ||
        ARRAY_TO_STRING(ARRAY_AGG(decade_themes_summary), ' ')
    ) AS overall_theme_evolution_summary
FROM {{ ref('themes_by_decade') }}

{{ with_test("not_null", column="overall_theme_evolution_summary") }}
