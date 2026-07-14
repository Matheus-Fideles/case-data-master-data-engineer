"""Unit tests for PII masking utilities."""
import os

import pytest

os.environ.setdefault("PII_SALT", "test-salt")


def test_mask_cpf_deterministic():
    from pipelines.common.masking import mask_cpf

    h1 = mask_cpf("123.456.789-00")
    h2 = mask_cpf("123.456.789-00")
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex


def test_mask_cpf_different_inputs():
    from pipelines.common.masking import mask_cpf

    assert mask_cpf("111.111.111-11") != mask_cpf("222.222.222-22")


def test_mask_cpf_none():
    from pipelines.common.masking import mask_cpf

    assert mask_cpf(None) is None
    assert mask_cpf("") is None


def test_validate_no_pii_passes(tmp_path):
    """validate_no_pii should not raise when PII columns absent."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    df = spark.createDataFrame([{"id": 1, "agravo": "dengue"}])
    from pipelines.common.masking import validate_no_pii
    validate_no_pii(df)  # must not raise
    spark.stop()


def test_validate_no_pii_raises():
    """validate_no_pii should raise when PII columns present."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    df = spark.createDataFrame([{"cpf": "12345678900", "id": 1}])
    from pipelines.common.masking import validate_no_pii
    with pytest.raises(ValueError, match="PII"):
        validate_no_pii(df)
    spark.stop()
