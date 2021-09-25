import sys
import os
sys.path.append(os.path.abspath(""))
from crawler import CrawlerCNPJ
from consolidation import CleanerCNPJ
from src.base import PublicSource
from src.util import *

import logging
logging.getLogger().setLevel(logging.INFO)

class CNPJSource(PublicSource):

    def __init__(self, spark_session, file_dir):
        
        self.spark = spark_session
        raw_dir = join_path(file_dir, 'data', 'raw')
        trusted_dir = join_path(file_dir, 'data', 'trusted')
        create_dir(raw_dir)
        create_dir(trusted_dir)
        self.crawler = CrawlerCNPJ(raw_dir)
        self.cleaner = CleanerCNPJ(spark_session, raw_dir, trusted_dir)

    def extract(self):

        logging.info("Extracting data...")
        self.crawler.run()

    def transform(self):
        
        logging.info("Consolidating tables...")
        self.cleaner.run()

    def load(self):

        logging.info("Writing data...")
        pass

    def create(self):

        self.extract()
        self.transform()
        self.load()
        logging.info("Success!")

