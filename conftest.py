import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()