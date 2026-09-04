"""Shared pytest fixtures. A local SparkSession with Delta for fast unit tests."""
import os
import pytest

os.environ.setdefault("TRADELENS_ENV", "local")


@pytest.fixture(scope="session")
def spark():
    from tradelens.common.spark_session import get_spark
    s = get_spark("tradelens-tests")
    yield s
    s.stop()
