from pyspark.sql import types as T

# Titles schema
class Titles:
    # Raw schema for titles - to be used in tests
    raw_schema = T.StructType([
        T.StructField("tconst", T.StringType(), True),
        T.StructField("titleType", T.StringType(), True),
        T.StructField("primaryTitle", T.StringType(), True),
        T.StructField("originalTitle", T.StringType(), True),
        T.StructField("isAdult", T.StringType(), True),
        T.StructField("startYear", T.StringType(), True),
        T.StructField("endYear", T.StringType(), True),
        T.StructField("runtimeMinutes", T.StringType(), True),
        T.StructField("genres", T.StringType(), True),
    ])

    # Schema for titles - to be used in the pipeline
    schema = T.StructType([
        T.StructField("tconst", T.StringType(), True),
        T.StructField("titleType", T.StringType(), True),
        T.StructField("primaryTitle", T.StringType(), True),
        T.StructField("originalTitle", T.StringType(), True),
        T.StructField("isAdult", T.IntegerType(), True),
        T.StructField("startYear", T.IntegerType(), True),
        T.StructField("endYear", T.IntegerType(), True),
        T.StructField("runtimeMinutes", T.IntegerType(), True),
        T.StructField("genres", T.StringType(), True),
    ])