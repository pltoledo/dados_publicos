from pyspark.sql import SparkSession
from cnpj.source import CNPJSource
import sys

spark = SparkSession \
                .builder \
                .config("spark.sql.broadcastTimeout", "360000") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .config('spark.sql.execution.arrow.pyspark.enabled', 'false') \
                .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")\
                .config("spark.driver.memory", '5G') \
                .config("spark.driver.maxResultSize", '8G') \
                .config('spark.sql.adaptive.enabled', 'true') \
                .config('spark.sql.legacy.parquet.datetimeRebaseModeInWrite', 'LEGACY') \
                .getOrCreate()

if __name__ == '__main__':
    if len(sys.argv[1::]) > 0:
        source = CNPJSource(spark, sys.argv[1])
        source.create()
        spark.stop()