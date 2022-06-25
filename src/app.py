import pandas as pd
import pyspark.pandas as ps
import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T

import warnings
warnings.filterwarnings(action='ignore', category=DeprecationWarning)

def pandas_add_column(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy().assign(new_column = 'a')

def pandas_on_spark_add_column(psdf: ps.DataFrame) -> ps.DataFrame:
    return psdf.assign(new_column = 'a')

def spark_add_column(sdf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return sdf.withColumn('new_column', F.lit('a'))

if __name__ == "__main__":

    spark = pyspark.sql.SparkSession.builder.appName("app").getOrCreate()
    
    # pandas
    df = pd.DataFrame({
        "id" : ['001', '002', '003']
    })

    result = pandas_add_column(df)
    print(result)

    # pandas or spark
    psdf = ps.DataFrame({
        "id" : ['001', '002', '003']
    })

    result = pandas_on_spark_add_column(psdf)
    print(result)

    # spark
    sdf = spark.createDataFrame(
        data = [
            ('001'),
            ('002'),
            ('003')
        ],
        schema = T.StringType()
    ).toDF('id')

    result = spark_add_column(sdf)
    print(result.show())