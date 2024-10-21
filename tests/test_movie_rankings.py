from pyspark.sql import SparkSession
from pyspark.sql import Row, functions as F, types as T
from schema.titles import Titles
from movie_rankings import generate_title_ranking, format_ratings_columns, get_top_10_movies
from chispa import assert_df_equality


def test_format_ratings_columns(spark: SparkSession):
    """ This is to test the convert_data_types function """

    # Create transformed schema for the ratings data
    ratings_schema = T.StructType([
        T.StructField("tconst", T.StringType(), True),
        T.StructField("averageRating", T.FloatType(), True),
        T.StructField("numVotes", T.IntegerType(), True),
    ])

    # Create test data
    test_data = [
        Row(tconst="tt0000001", averageRating="5.6", numVotes="1500"),
        Row(tconst="tt0000002", averageRating="7.2", numVotes="3000"),
        Row(tconst="tt0000003", averageRating="8.1", numVotes="500"),
        Row(tconst="tt0000004", averageRating="6.4", numVotes="1200"),
    ]

    ratings_df = spark.createDataFrame(test_data)

    # Test the function
    actual_data = format_ratings_columns(ratings_df)

    # Create expected data
    expected_data = spark.createDataFrame([
        Row(tconst="tt0000001", averageRating=5.6, numVotes=1500),
        Row(tconst="tt0000002", averageRating=7.2, numVotes=3000),
        Row(tconst="tt0000003", averageRating=8.1, numVotes=500),
        Row(tconst="tt0000004", averageRating=6.4, numVotes=1200),
    ], schema=ratings_schema)

    # Assert the data
    assert_df_equality(
        df1=actual_data,
        df2=expected_data,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True
    )


def test_generate_title_ranking(spark: SparkSession):
    """ This is to test the generate_title_ranking function """

    # Create a schema for the test data
    test_schema = T.StructType([
        T.StructField("numVotes", T.IntegerType(), True),
        T.StructField("averageRating", T.FloatType(), True),
        T.StructField("averageNumberOfVotes", T.FloatType(), True),
    ])

    # Test data
    test_data = [
        Row(numVotes=1500, averageRating=5.6, averageNumberOfVotes=1000.0),
        Row(numVotes=3000, averageRating=7.2, averageNumberOfVotes=2000.0),
        Row(numVotes=500, averageRating=8.1, averageNumberOfVotes=500.0),
        Row(numVotes=1200, averageRating=6.4, averageNumberOfVotes=1000.0),
    ]

    # Create a DataFrame from the list of Rows
    test_df = spark.createDataFrame(test_data, schema=test_schema)

    # Apply the UDF
    result_df = test_df.withColumn(
        "ranking", generate_title_ranking(F.col("numVotes"), F.col("averageRating"), F.col("averageNumberOfVotes"))
    )

    # Create expected data
    expected_data = [
        Row(numVotes=1500, averageRating=5.6, averageNumberOfVotes=1000.0, ranking=8.4),
        Row(numVotes=3000, averageRating=7.2, averageNumberOfVotes=2000.0, ranking=10.799999),
        Row(numVotes=500, averageRating=8.1, averageNumberOfVotes=500.0, ranking=8.1),
        Row(numVotes=1200, averageRating=6.4, averageNumberOfVotes=1000.0, ranking=7.6800003),
    ]

    # Create a DataFrame from the expected data
    expected_df = spark.createDataFrame(expected_data, schema=test_schema.add("ranking", T.FloatType()))

    # Assert the data
    assert_df_equality(
        df1=result_df,
        df2=expected_df,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True
    )


def test_get_top_10_movies(spark: SparkSession):
    """ This is to test the get_top_10_movies function """

    # Create schema for titles data
    title_schema = T.StructType([
        T.StructField("tconst", T.StringType(), True),
        T.StructField("primaryTitle", T.StringType(), True),
        T.StructField("originalTitle", T.StringType(), True),
    ])

    # Create schema for ratings data
    ratings_schema = T.StructType([
        T.StructField("tconst", T.StringType(), True),
        T.StructField("averageRating", T.FloatType(), True),
        T.StructField("numVotes", T.IntegerType(), True),
    ])

    # Create test data for titles
    titles_df = spark.createDataFrame([
        Row(tconst="tt0000001", primaryTitle="some_title_1", originalTitle="some_title_1"),
        Row(tconst="tt0000002", primaryTitle="some_title_2", originalTitle="some_title_2"),
        Row(tconst="tt0000003", primaryTitle="some_title_3", originalTitle="some_title_3"),
        Row(tconst="tt0000004", primaryTitle="some_title_4", originalTitle="some_title_4"),
        Row(tconst="tt0000005", primaryTitle="some_title_5", originalTitle="some_title_5"),
    ], schema=title_schema)

    # Create test data for ratings
    ratings_df = spark.createDataFrame([
        Row(tconst="tt0000001", averageRating=5.6, numVotes=1500),
        Row(tconst="tt0000002", averageRating=7.2, numVotes=3000),
        Row(tconst="tt0000003", averageRating=8.1, numVotes=500),
        Row(tconst="tt0000004", averageRating=6.4, numVotes=1200),
        Row(tconst="tt0000005", averageRating=9.0, numVotes=2000),
    ],schema=ratings_schema)

    # Calling the function to test
    top_10_movies_df = get_top_10_movies(titles_df=titles_df, ratings_df=ratings_df)

    # Create expected data
    expected_df = spark.createDataFrame([
        Row(rank=1, tconst="tt0000002", primaryTitle="some_title_2"),
        Row(rank=2, tconst="tt0000005", primaryTitle="some_title_5"),
        Row(rank=3, tconst="tt0000001", primaryTitle="some_title_1"),
        Row(rank=4, tconst="tt0000004", primaryTitle="some_title_4"),
        Row(rank=5, tconst="tt0000003", primaryTitle="some_title_3"),
    ], schema=T.StructType([
        T.StructField("rank", T.IntegerType(), True),
        T.StructField("tconst", T.StringType(), True),
        T.StructField("primaryTitle", T.StringType(), True),
    ])
    )

    assert_df_equality(
        df1=top_10_movies_df,
        df2=expected_df,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True
    )
