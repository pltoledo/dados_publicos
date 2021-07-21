from crawler import CrawlerCNPJ
from consolidation import CleanCNPJ
import sys

import findspark
findspark.init()

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession

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

def main(spark_session, file_dir, save_dir):
    print('Running crawler...')
    crawler = CrawlerCNPJ(file_dir)
    crawler.get_data()
    crawler.unzip()
    print('Cleaning data...')
    cleaner = CleanCNPJ(spark_session, file_dir, save_dir)
    cleaner.run()
    print('All done!')



if __name__ == '__main__':
    if len(sys.argv[1::]) > 0:
        main(spark, sys.argv[1], sys.argv[2])
        spark.stop()