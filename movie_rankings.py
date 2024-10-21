import time
from typing import List, Optional

from pyspark.sql import (
    DataFrame,
    SparkSession,
    Window as W,
    functions as F,
    types as T
)
from helpers.constants import conf
from helpers.utils import ArgumentParserCheck

NUM_OF_TITLE_HASH_PARTITION = 200

@F.udf(returnType=T.FloatType())
def generate_title_ranking(
        numVotes: int,
        averageRating: float,
        averageNumberOfVotes: float
) -> DataFrame:
    """ This function will generate the title ranking
    :param numVotes: int - The number of votes
    :param averageRating: float - The average rating
    :param averageNumberOfVotes: float - The average number of votes
    """
    return (numVotes/averageNumberOfVotes) * averageRating

def format_ratings_columns(ratings_df: DataFrame) -> DataFrame:
    """ This function will convert the data types of the ratings data
    :param ratings_df: DataFrame - The ratings data
    :return: DataFrame - The ratings data with the correct data types
    """

    ratings_df = (
        ratings_df
        .withColumn("averageRating", F.col("averageRating").cast(T.FloatType()))
        .withColumn("numVotes", F.col("numVotes").cast(T.IntegerType()))
    )

    return ratings_df

def get_top_10_movies(
    titles_df: DataFrame,
    ratings_df: DataFrame
) -> DataFrame:
    """"  This function returns the top 10 movies based on the ranking
    :param titles_df: DataFrame - The titles data
    :param ratings_df: DataFrame - The ratings data
    :return: DataFrame - The top 10 movies
    """
    titles_with_ratings_df = titles_df.join(ratings_df, on='tconst', how="inner")
    average_number_of_votes = titles_with_ratings_df.agg(F.avg(F.col("numVotes")).alias("averageNumberOfVotes")).collect()[0]["averageNumberOfVotes"]

    rank_spec  = W.partitionBy("dummy_partition").orderBy(F.col("ranking").desc())

    return (
        titles_with_ratings_df
        .withColumn("dummy_partition", F.lit(1)) # to supress the window function warning
        .withColumn("ranking", generate_title_ranking(F.col("numVotes"), F.col("averageRating"), F.lit(average_number_of_votes)))
        .withColumn("rank", F.row_number().over(rank_spec))
        .orderBy(F.col("ranking").desc())
        # .withColumn("rank_v2", F.monotonically_increasing_id() + 1)
        .filter(F.col("rank") <= 10)
        .select("rank", "tconst", "primaryTitle")
    )


def get_most_credited(
        names_df: DataFrame,
        principal_df: DataFrame,
        top_10_movies_df: DataFrame
        ) -> DataFrame:

    """ This function will return top 10 movies along with the most credited persons
    :param names_df: DataFrame - The names data
    :param principal_df: DataFrame - The principal data
    :param top_10_movies_df: DataFrame - The top 10 movies
    :return: DataFrame - The top 10 movies with the most credited persons
    """
    movies_with_principals = (
        top_10_movies_df
        .join(principal_df, on="tconst", how="inner")
    )

    actor_category = (
        movies_with_principals
        .groupBy("tconst", "nconst")
        .agg(F.collect_list("category").alias("credit_category"))
        .select("tconst", "nconst", "credit_category")
    )

    window_spec = W.partitionBy("tconst").orderBy(F.col("credit_count").desc())

    principal_with_credit_count = (
        movies_with_principals
        .groupBy("tconst", "nconst")
        .agg(F.countDistinct("category").alias("credit_count"))
        .withColumn("credit_rank", F.row_number().over(window_spec))
        .filter(F.col("credit_rank") == 1)
        .select("tconst", "nconst", "credit_count")
    )
    return (
        principal_with_credit_count
        .join(top_10_movies_df, on="tconst", how="inner")
        .join(names_df, on="nconst", how="inner")
        .join(actor_category, on=["tconst", "nconst"], how="inner")
        .orderBy(F.col("rank").asc(), F.col("credit_count").desc())
        .withColumnRenamed("rank", "movie_rank")
        .select("tconst", "nconst", "movie_rank", "primaryTitle", "primaryName", "credit_count", "credit_category")
    )


def run_pipeline(
        spark: SparkSession,
        titles_path: str,
        ratings_path: str,
        principals_path: str,
        names_path: str,
        ) -> None:
    """ This function will calculate the top 10 movies """

    start_time = time.time()

    # Load titles data with the required columns
    titles_df = (
        spark
        .read
        .option("sep", "\t")
        .csv(titles_path, header=True)
        .filter(F.col("titleType") == "movie") # Filter only movies
        .select(
            "tconst",
            "primaryTitle",
            "originalTitle"
        )
    )

    # Load ratings data
    ratings_df = (
        spark
        .read
        .option("sep", "\t")
        .csv(ratings_path, header=True)
    )

    # load names data
    names_df = (
        spark
        .read
        .option("sep", "\t")
        .csv(names_path, header=True)
        .select("nconst", "primaryName")
    )

    principal_df = (
        spark
        .read
        .option("sep", "\t")
        .csv(principals_path, header=True)
        .select("tconst", "nconst", "category", "ordering")
        .repartition(NUM_OF_TITLE_HASH_PARTITION, "tconst") # Repartition the data - mainly for join operation in a cluster
    )

    ratings_transformed = format_ratings_columns(ratings_df).filter(F.col("numVotes") >= 500)

    top_10_movies_df = get_top_10_movies(
        titles_df=titles_df,
        ratings_df=ratings_transformed
    )

    top_10_movies_with_most_credited_actor = get_most_credited(
        names_df=names_df,
        principal_df=principal_df,
        top_10_movies_df=top_10_movies_df
    )

    print("Top 10 Movies")
    top_10_movies_df.show(truncate=False)

    print("Top 10 movies with most credited actors")
    top_10_movies_with_most_credited_actor.show(truncate=False)

    end_time = time.time()
    print(f"Total time took: {(end_time - start_time)/60}")



def entrypoint(spark: SparkSession, commmand_line_args: Optional[List[str]] = None) -> None:
    """ Entrypoint for the top 10 movies """

    parser = ArgumentParserCheck(description="Top 10 Movies")

    parser.add_argument("--titles-path", required=False, help="Path to the titles data", default="data/title.basics.tsv")
    parser.add_argument("--ratings-path", required=False, help="Path to the ratings data", default="data/title.ratings.tsv")
    parser.add_argument("--names-path", required=False, help="Path to the names data", default="data/name.basics.tsv")
    parser.add_argument("--principals-path", required=False, help="Path to the principals data", default="data/title.principals.tsv")

    known_args, _= parser.parse_known_args(commmand_line_args)

    run_pipeline(
        spark,
        titles_path=known_args.titles_path,
        ratings_path=known_args.ratings_path,
        principals_path=known_args.principals_path,
        names_path=known_args.names_path
    )

if __name__ == "__main__":

    entrypoint(spark=SparkSession
        .builder
        .appName("imdb_app")
        .master("local[2]")
        .config(conf=conf)
        .getOrCreate()
    )
