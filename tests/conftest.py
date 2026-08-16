import pytest
import sys
import os

SparkSession = pytest.importorskip("pyspark.sql").SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (SparkSession.builder.master("local[2]")
               .appName("zingg-native-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.pyspark.python", sys.executable)
               .config("spark.pyspark.driver.python", sys.executable)
               .getOrCreate())
    yield session
    session.stop()
