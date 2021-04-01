from crawler import CrawlerComex
from consolidation import CleanComex
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
            .config("spark.driver.memory", '14G') \
            .config("spark.executor.memory", '14G') \
            .config("spark.driver.maxResultSize", '4G') \
            .getOrCreate()

def main(file_dir, save_dir):
    print('Running crawler...')
    crawler = CrawlerComex(file_dir)
    crawler.get_data()
    print('Cleaning data...')
    cleaner = CleanComex(spark, file_dir, save_dir)
    cleaner.run()
    print('All done!')



if __name__ == '__main__':
    if type(sys.argv[1]) is str and type(sys.argv[2]) is str:
        main(sys.argv[1], sys.argv[2])