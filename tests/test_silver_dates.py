"""Conversion des heures SNCF (locales) en UTC dans Silver."""
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format

from src.streaming.silver_transform import parse_sncf_timestamp


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-dates")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _en_utc(spark, texte):
    df = spark.createDataFrame([(texte,)], ["t"])
    ligne = df.select(
        date_format(parse_sncf_timestamp(col("t")), "yyyy-MM-dd HH:mm:ss").alias("utc")
    ).first()
    return ligne["utc"]


def test_heure_locale_hiver_vers_utc(spark):
    assert _en_utc(spark, "20261120T083758") == "2026-11-20 07:37:58"


def test_heure_locale_ete_vers_utc(spark):
    assert _en_utc(spark, "20260820T083758") == "2026-08-20 06:37:58"
