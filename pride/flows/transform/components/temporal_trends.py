import pandas as pd
from typing import Optional
from ascend.application.context import ComponentExecutionContext
from ascend.common.events import log
from ascend.resources import ref, test, transform

@transform(
    inputs=[
        ref("read_films", flow="extract-load"),
    ],
    input_data_format="pandas",
    tests=[
        test("count_greater_than", count=0),
    ]
)
def analyze_release_trends(read_films: pd.DataFrame, context: ComponentExecutionContext) -> pd.DataFrame:
    """
    Analyze temporal trends and seasonality in film releases.
    - Release Trends: Films per year and decade.
    - Seasonality: Films per month.
    """
    df = read_films.copy()
    log(f"Loaded {len(df)} films for trend analysis.")

    # Ensure RELEASE_DATE is datetime
    df["RELEASE_DATE"] = pd.to_datetime(df["RELEASE_DATE"], errors="coerce")
    df = df.dropna(subset=["RELEASE_DATE"])

    # Year and Decade
    df["year"] = df["RELEASE_DATE"].dt.year
    df["decade"] = (df["year"] // 10) * 10

    # Month
    df["month"] = df["RELEASE_DATE"].dt.month

    # Release Trends: Films per year and decade
    films_per_year = df.groupby("year").size().reset_index(name="films_per_year")
    films_per_decade = df.groupby("decade").size().reset_index(name="films_per_decade")

    # Seasonality: Films per month
    films_per_month = df.groupby("month").size().reset_index(name="films_per_month")

    # Prepare output DataFrame
    films_per_year["type"] = "year"
    films_per_decade["type"] = "decade"
    films_per_month["type"] = "month"
    films_per_year = films_per_year.rename(columns={"year": "period", "films_per_year": "count"})
    films_per_decade = films_per_decade.rename(columns={"decade": "period", "films_per_decade": "count"})
    films_per_month = films_per_month.rename(columns={"month": "period", "films_per_month": "count"})

    result = pd.concat([films_per_year, films_per_decade, films_per_month], ignore_index=True)

    log("Temporal trend analysis complete.")
    return result