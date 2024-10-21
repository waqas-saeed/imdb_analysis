from tempfile import mkdtemp
import shutil
import pytest
from pyspark.sql import SparkSession


def _get_spark_session() -> SparkSession:
    return (
        SparkSession
        .builder
        .appName("testing")
        .master("local[2]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.executorEnv.PYTHONHASHSEED", 0)
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "Europe/London")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def spark(request):
    spark = _get_spark_session()
    request.addfinalizer(lambda: spark.stop())
    yield spark


@pytest.fixture(scope="function")
def tmp_dir():
    output_dir = mkdtemp()
    yield output_dir
    shutil.rmtree(output_dir)
