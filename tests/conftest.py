import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return Sparksession.builder.getOrCreate()