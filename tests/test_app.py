from src.app import pandas_add_column, pandas_on_spark_add_column, spark_add_column
import pandas as pd
import pyspark.pandas as ps
import pytest
from pyspark.sql import SparkSession
import pyspark.sql.types as T
from chispa.dataframe_comparer import assert_df_equality

@pytest.mark.pandas
def test_pandas_add_column():
    
    # ARRANGE
    df = pd.DataFrame({
        "id" : ['001', '002', '003']
    })

    expect = pd.DataFrame({
        "id" : ['001', '002', '003'],
        "new_column" : ['a', 'a', 'a']
    })
    # ACT
    actual = pandas_add_column(df)

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)

@pytest.mark.pandas
def test_pandas_add_column_fail_case():
    
    # ARRANGE
    df = pd.DataFrame({
        "id" : ['001', '002', '003']
    })

    expect = pd.DataFrame({
        "id" : ['001', '002', '003'],
        "new_column" : ['b', 'b', 'b']
    })
    # ACT
    actual = pandas_add_column(df)

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)


@pytest.mark.pandas
def test_pandas_add_column_with_fixture(pandas_mock_df):
    
    # ARRANGE
    df, expect = pandas_mock_df

    # ACT
    actual = pandas_add_column(df)

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)

@pytest.mark.spark
def test_pandas_on_spark_add_column(spark, pandas_on_spark_mock_df):
    
    # ARRANGE
    df, expect = pandas_on_spark_mock_df

    # ACT
    actual = pandas_on_spark_add_column(df).to_pandas()

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)

@pytest.mark.spark
def test_spark_add_column_pandas_testing(spark, spark_mock_df):
    
    # ARRANGE
    df, expect = spark_mock_df

    # ACT
    actual = spark_add_column(df).toPandas()

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)

@pytest.mark.spark
def test_spark_add_column_pandas_testing_failed(spark, spark_mock_df_failed):
    
    # ARRANGE
    df, expect = spark_mock_df_failed

    # ACT
    actual = spark_add_column(df).toPandas()

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)

@pytest.mark.spark
def test_spark_add_column_chispa(spark, spark_mock_df_chispa):
    
    # ARRANGE
    df, expect = spark_mock_df_chispa

    # ACT
    actual = spark_add_column(df)

    # ASSERT
    assert_df_equality(actual, expect)

@pytest.mark.spark
def test_spark_add_column_chispa_failed(spark, spark_mock_df_chispa_failed):
    
    # ARRANGE
    df, expect = spark_mock_df_chispa_failed

    # ACT
    actual = spark_add_column(df)

    # ASSERT
    assert_df_equality(actual, expect)


@pytest.mark.with_spark_context
@pytest.mark.parametrize('num_test', range(30))
def test_spark_add_column_with_spark_context(num_test, spark, spark_mock_df):
    
    # ARRANGE
    df, expect = spark_mock_df

    # ACT
    actual = spark_add_column(df).toPandas()

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)

@pytest.mark.without_spark_context
@pytest.mark.parametrize('num_test', range(30))
def test_spark_add_column_without_spark_context(num_test):
    
    # ARRANGE
    spark = SparkSession.builder.appName("pytest-spark").getOrCreate()

    sdf = spark.createDataFrame(
        data = [
            ('001'),
            ('002'),
            ('003')
        ],
        schema = T.StringType()
    ).toDF('id')

    expect = pd.DataFrame({
        "id" : ['001', '002', '003'],
        "new_column" : ['a', 'a', 'a']
    })

    # ACT
    actual = spark_add_column(sdf).toPandas()

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)

    spark.stop()