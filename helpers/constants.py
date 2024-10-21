from pyspark import SparkConf
from pyspark.sql import types as T, functions as F, Window

conf = SparkConf()
conf.setAll([
    ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4"),
    ("spark.sql.sources.partitionOverwriteMode", "dynamic"),
    ("spark.executorEnv.PYTHONHASHSEED", 0),
    ("spark.sql.shuffle.partitions", 1),
    ("spark.sql.autoBroadcastJoinThreshold", -1),
    ("spark.driver.memory", "2g"),
    ("spark.sql.session.timeZone", "Europe/London"),
    ("spark.driver.host", "localhost"),
])


