from ascend.resources import ref, transform, test
from ascend.application.context import ComponentExecutionContext
from ascend.common.events import log
import polars as pl

@transform(
    inputs=[
        ref("read_films", flow="extract-load"),
        ref("read_genres", flow="extract-load")
    ],
    input_data_format="pandas",
    tests=[
        test("not_null", column="OVERVIEW"),
        test("not_null", column="RELEASE_DATE"),
        test("not_empty", column="ORIGINAL_TITLE"),
        test("count_greater_than", count=0),
    ]
)
def map_genres(read_films, read_genres, context: ComponentExecutionContext):
    """
    Transforms film records by mapping genre IDs to genre names and cleaning the data.

    - Drops any films with missing release dates or missing overviews.
    - Parses the 'GENRE_IDS' column from a stringified list to a list of integers.
    - Joins with the genres reference to map each genre ID to its name.
    - Produces a DataFrame where each film has a 'genre_name' column containing a list of genre names.
    - Ensures data quality with tests on key columns.

    Args:
        read_films: DataFrame of films, including 'GENRE_IDS', 'overview', 'RELEASE_DATE', etc.
        read_genres: DataFrame of genres, including 'ID' and 'NAME'.
        context: Ascend component execution context.

    Returns:
        pd.DataFrame: Films with genre names mapped and cleaned.
    """
    # Convert to Polars
    films_df = pl.from_pandas(read_films) if not isinstance(read_films, pl.DataFrame) else read_films
    genres_df = pl.from_pandas(read_genres) if not isinstance(read_genres, pl.DataFrame) else read_genres

    # Drop rows with missing release dates or null overview
    films_df = films_df.filter(
        pl.col("RELEASE_DATE").is_not_null() & pl.col("OVERVIEW").is_not_null()
    )

    # Log columns to debug some column handling errors
    log(f"Columns in films_df: {films_df.columns}")
    
    # Step 1: Clean and split genre_ids string into list of integers with proper edge case handling
    films_df = films_df.with_columns(
        pl.when(pl.col("GENRE_IDS").is_null() | (pl.col("GENRE_IDS") == ""))
        .then(pl.lit(None, dtype=pl.List(pl.Int64)))  # Handle null/empty as empty list
        .otherwise(
            pl.col("GENRE_IDS")
            .str.replace_all(r"[^\d,]", "")  # Remove everything except digits and commas
            .str.split(",")
            .list.eval(
                pl.element()
                .filter(pl.element() != "")  # Filter out empty strings
                .filter(pl.element().str.len_chars() > 0)  # Additional check for non-empty
                .cast(pl.Int64, strict=False)  # Use non-strict casting to handle conversion errors
                .filter(pl.element().is_not_null())  # Remove any nulls from failed conversions
            )
        )
        .alias("genre_ids")
    )

    # Step 2: Normalize genre column names
    genres_df = genres_df.rename({"ID": "genre_id", "NAME": "genre_name"})
    genres_df = genres_df.with_columns(pl.col("genre_id").cast(pl.Int64))

    # Step 3: Explode genre_ids and join with genre names
    # Handle cases where genre_ids might be null or empty
    exploded = (films_df
                .select(["genre_ids"])
                .with_row_count("row_nr")
                .filter(pl.col("genre_ids").is_not_null())  # Filter out null genre_ids
                .filter(pl.col("genre_ids").list.len() > 0)  # Filter out empty lists
                .explode("genre_ids"))
    
    # Only proceed with join if we have data to join
    if exploded.height > 0:
        joined = exploded.join(genres_df, left_on="genre_ids", right_on="genre_id", how="left")
        # Step 4: Group back by row and collect genre names
        genre_names = joined.group_by("row_nr").agg(pl.col("genre_name")).sort("row_nr")
    else:
        # Create empty genre_names DataFrame with correct schema
        genre_names = pl.DataFrame({
            "row_nr": pl.Series([], dtype=pl.UInt32),
            "genre_name": pl.Series([], dtype=pl.List(pl.Utf8))
        })

    # Step 5: Add genres back to original films_df
    films_df = films_df.with_row_count("row_nr").join(genre_names, on="row_nr", how="left").drop("row_nr")
    
    # Fill null genre lists with empty lists
    films_df = films_df.with_columns(
        pl.col("genre_name").fill_null(pl.lit([], dtype=pl.List(pl.Utf8)))
    )

    return films_df.to_pandas()
