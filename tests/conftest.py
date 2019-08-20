""" pytest fixture that can be reused across tests """

import pytest
from _pytest.fixtures import FixtureRequest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session(request: FixtureRequest) -> SparkSession:
    """
    Create a pytest SparkSession fixture for testing.
    :param request: pytest FixtureRequest object
    :return: A SparkSession
    """
    spark = (SparkSession.builder
             .master("local")
             .appName("test_pyspark_session")
             .getOrCreate())

    # Teardown spark session between module tests
    request.addfinalizer(lambda: spark.stop())
    return spark
