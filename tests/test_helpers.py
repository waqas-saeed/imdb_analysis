from pyspark.sql import SparkSession, Row
from helpers.utils import fetch_and_clean_titles_data
from schema.titles import Titles
from chispa import assert_df_equality


def test_fetch_and_clean_titles_data(spark: SparkSession, tmp_dir: str):
    """ This is to test data cleanup function for titles dataset where /N wll be replaced with None
    and data type will be converted to appropriate data type
    spark: SparkSession - The spark session
    tmp_dir: str - Temporary directory to store the test data
    """

    titles_source_path = f"{tmp_dir}/title.basics.tsv"
    # Create a list of Rows
    test_data = spark.createDataFrame([
        Row(tconst="tt0000001", titleType="short", primaryTitle="Carmencita", originalTitle="Carmencita", isAdult="0", startYear="\\N", endYear=None, runtimeMinutes="1", genres="Documentary,Short"),
        Row(tconst="tt0000002", titleType="short", primaryTitle="Le clown et ses chiens", originalTitle="Le clown et ses chiens", isAdult="0", startYear="1892", endYear=None, runtimeMinutes="5", genres="Animation,Short"),
        Row(tconst="tt0000003", titleType="short", primaryTitle="Poor Pierrot", originalTitle="Pauvre Pierrot", isAdult="0", startYear="1892", endYear="\\N", runtimeMinutes="5", genres="Animation,Comedy,Romance"),
        Row(tconst="tt0000004", titleType="short", primaryTitle="Un bon bock", originalTitle="Un bon bock", isAdult="\\N", startYear="1892", endYear=None, runtimeMinutes="\\N", genres="Animation,Short"),
    ], schema=Titles.raw_schema
    )

    # write the test data to a file
    test_data.write.csv(titles_source_path, mode="overwrite", header=True, sep="\t")

    # Test the function
    actual_data = fetch_and_clean_titles_data(spark=spark, titles_path=titles_source_path)

    # Create expected data
    expected_data = spark.createDataFrame([
        Row(tconst="tt0000001", titleType="short", primaryTitle="Carmencita", originalTitle="Carmencita", isAdult=0, startYear=None, endYear=None, runtimeMinutes=1, genres="Documentary,Short"),
        Row(tconst="tt0000002", titleType="short", primaryTitle="Le clown et ses chiens", originalTitle="Le clown et ses chiens", isAdult=0, startYear=1892, endYear=None, runtimeMinutes=5, genres="Animation,Short"),
        Row(tconst="tt0000003", titleType="short", primaryTitle="Poor Pierrot", originalTitle="Pauvre Pierrot", isAdult=0, startYear=1892, endYear=None, runtimeMinutes=5, genres="Animation,Comedy,Romance"),
        Row(tconst="tt0000004", titleType="short", primaryTitle="Un bon bock", originalTitle="Un bon bock", isAdult=None, startYear=1892, endYear=None, runtimeMinutes=None, genres="Animation,Short"),
    ], schema=Titles.schema
    )

    # Assert the data
    assert_df_equality(
        df1=actual_data,
        df2=expected_data,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True
    )
