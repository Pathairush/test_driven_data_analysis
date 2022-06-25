from src.app import pandas_add_column, pandas_on_spark_add_column, spark_add_column
import pandas as pd
import pyspark.pandas as ps

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
    
def test_pandas_add_column_with_fixture(pandas_mock_df):
    
    # ARRANGE
    df, expect = pandas_mock_df

    # ACT
    actual = pandas_add_column(df)

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)

def test_pandas_on_spark_add_column(spark, pandas_on_spark_mock_df):
    
    # ARRANGE
    df, expect = pandas_on_spark_mock_df

    # ACT
    actual = pandas_on_spark_add_column(df).to_pandas()

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)

def test_spark_add_column(spark, spark_mock_df):
    
    # ARRANGE
    df, expect = spark_mock_df

    # ACT
    actual = spark_add_column(df).toPandas()

    # ASSERT
    pd.testing.assert_frame_equal(actual, expect, check_exact=True)
