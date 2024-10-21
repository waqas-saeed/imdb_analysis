import argparse as argparse
from pyspark.sql import SparkSession, DataFrame, types as T, functions as F

class ArgumentParserCheck(argparse.ArgumentParser):
    def error(self, message):
        raise Exception(message)



def fetch_and_clean_titles_data(spark: SparkSession, titles_path: str) -> DataFrame:
    """ This function will load the all columns in titles data and clean it
    :param spark: SparkSession - The spark session
    :param titles_path: str - Path to the titles data
    :return: DataFrame - The cleaned titles data
    """

    titles_df = (
        spark
        .read
        .option("sep", "\t")
        .csv(titles_path, header=True)
    )

    # Clean the data
    titles_df = (
        titles_df
        .withColumn('startYear',
                    F.when(F.col("startYear") == "\\N", None).otherwise(F.col("startYear")).cast(T.IntegerType())
        )
        .withColumn('endYear',
                    F.when(F.col("endYear") == "\\N", None).otherwise(F.col("endYear")).cast(T.IntegerType())
        )
        .withColumn('isAdult',
                    F.when(F.col("isAdult") == "\\N", None).otherwise(F.col("isAdult")).cast(T.IntegerType())
        )
        .withColumn(
            'runtimeMinutes', F.when(F.col("runtimeMinutes") == "\\N", None).otherwise(F.col("runtimeMinutes")).cast(T.IntegerType())
        )
    )

    return titles_df