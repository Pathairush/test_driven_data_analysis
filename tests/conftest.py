import pytest
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pandas as pd
import pyspark.pandas as ps

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName("pytest-spark").getOrCreate()

@pytest.fixture
def pandas_mock_df():

    df = pd.DataFrame({
        "id" : ['001', '002', '003']
    })

    expect = pd.DataFrame({
        "id" : ['001', '002', '003'],
        "new_column" : ['a', 'a', 'a']
    })

    return df, expect

@pytest.fixture
def pandas_on_spark_mock_df():

    df = ps.DataFrame({
        "id" : ['001', '002', '003']
    })
    
    expect = pd.DataFrame({
        "id" : ['001', '002', '003'],
        "new_column" : ['a', 'a', 'a']
    })

    return df, expect

@pytest.fixture
def spark_mock_df(spark):

    df = spark.createDataFrame(
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

    return df, expect

@pytest.fixture
def spark_mock_df_failed(spark):

    df = spark.createDataFrame(
        data = [
            ('001'),
            ('002'),
            ('003')
        ],
        schema = T.StringType()
    ).toDF('id')

    expect = pd.DataFrame({
        "id" : ['001', '002', '003'],
        "new_column" : ['b', 'b', 'b']
    })

    return df, expect

@pytest.fixture
def spark_mock_df_chispa(spark):

    df = spark.createDataFrame(
        data = [
            ('001'),
            ('002'),
            ('003')
        ],
        schema = T.StringType()
    ).toDF('id')

    expect = spark.createDataFrame(
        data = [
            ('001', 'a'),
            ('002', 'a'),
            ('003', 'a'),
        ],
        schema = T.StructType([
            T.StructField('id', T.StringType(), True),
            T.StructField('new_column', T.StringType(), False),
        ])
    )

    return df, expect

@pytest.fixture
def spark_mock_df_chispa_failed(spark):

    df = spark.createDataFrame(
        data = [
            ('001'),
            ('002'),
            ('003')
        ],
        schema = T.StringType()
    ).toDF('id')

    expect = spark.createDataFrame(
        data = [
            ('001', 'b'),
            ('002', 'b'),
            ('003', 'b'),
        ],
        schema = T.StructType([
            T.StructField('id', T.StringType(), True),
            T.StructField('new_column', T.StringType(), False),
        ])
    )

    return df, expect

@pytest.fixture
def spark_multi_column_mock_df(spark):

    sdf = spark.createDataFrame(
        data = [
            ('001', 'a'),
            ('002', 'b'),
            ('003', 'C'),
        ],
        schema = T.StructType([
            T.StructField('id', T.StringType(), False),
            T.StructField('symbol', T.StringType(), False),
        ])
    )

    return sdf

