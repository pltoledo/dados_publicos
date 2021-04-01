import numpy as np
import pandas as pd
import requests
import os
import zipfile
from bs4 import BeautifulSoup
import sys

class CrawlerCNPJ:
    def __init__(self, save_dir) -> None:
        self.base_url = 'http://200.152.38.155/CNPJ/'
        self.save_dir = save_dir
        r = requests.get(self.base_url)
        soup = BeautifulSoup(r.content, 'html.parser')
        self.files = [i['href'] for i in soup.select('a', href=True) 
                      if i['href'].endswith('.zip')]

    def download_url(self, url, save_path, chunk_size=128):
        r = requests.get(url)
        with open(save_path, 'wb') as fd:
            fd.write(r.content)
    
    def get_data(self, overwrite = True):
        for file in self.files:
            url = self.base_url + file
            save_path = os.path.join(self.save_dir, file)
            if not os.path.exists(save_path):
                self.download_url(url, save_path)
            elif overwrite:
                self.download_url(url, save_path)
            else:
                continue

    def unzip(self):
        for file in self.files:   
            filepath = os.path.join(self.save_dir, file).replace("\\","/")
            newpath = os.path.join(self.save_dir, file.replace('.zip', ''))
            if os.path.exists(filepath):
                zip_ref = zipfile.ZipFile(filepath) # create zipfile object
                zip_ref.extractall(newpath) # extract file to dir
                zip_ref.close() # close file
                os.remove(filepath) # delete zipped file
            else:
                pass

if __name__ == '__main__':
    if type(sys.argv[1]) is str:
        crawler = CrawlerCNPJ(sys.argv[1])
        crawler.get_data()
        crawler.unzip()
    else:
        raise Exception("O argumento deve ser um string com o caminho de destino")