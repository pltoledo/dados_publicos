import requests
from bs4 import BeautifulSoup
import re
import os
import sys
import csv
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class CrawlerComex:
    
    def __init__(self, save_dir, years = (2016, 2021)):
        URL = 'https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta'
        r = requests.get(URL)
        soup = BeautifulSoup(r.content, 'html.parser')
        files = [i['href'] for i in soup.select('a', href = True) 
                 if i.get('href') 
                 and i['href'].endswith('.csv') 
                 and (i['href'].find('mun') > -1)]
        range_years = range(years[0], years[1] + 1)
        files = [i for i in files if (re.search('[0-9]{4}', i) and int(re.search('[0-9]{4}', i).group(0)) in range_years)]
        self.create_dir(save_dir)
        self.files = files
        self.save_dir = save_dir
    
    def download_data(self, url, save_path):
        r = requests.get(url, verify = False)
        with open(save_path, 'w', newline='') as f:
            writer = csv.writer(f, quotechar='"')
            for line in r.iter_lines():
                writer.writerow(line.decode('utf-8').replace('"', '').split(';'))
        
    def create_dir(self, path):
        if not os.path.exists(path):
            os.mkdir(path)
    
    def get_data(self):
        ## Tabelas principais
        mun_path = os.path.join(self.save_dir, 'mun').replace("\\","/")
        self.create_dir(mun_path)
        for file in self.files:
            filename = re.search('(?<=mun/).+', file).group(0)
            save_path = os.path.join(mun_path, filename).replace("\\","/")
            self.download_data(file, save_path)
        
        ## Tabelas auxiliares
        aux_path = os.path.join(self.save_dir, 'aux_tables').replace("\\","/")
        self.create_dir(aux_path)
        TABELAS_AUX = 'https://balanca.economia.gov.br/balanca/bd/tabelas/TABELAS_AUXILIARES.xlsx'
        pd.read_excel(TABELAS_AUX, sheet_name = 1) \
          .to_csv(os.path.join(aux_path, 'NCM_SH.csv').replace('\\', '/'), header = True, index = False)
        pd.read_excel(TABELAS_AUX, sheet_name = 14) \
          .to_csv(os.path.join(aux_path, 'UF_MUN.csv').replace('\\', '/'), header = True, index = False)
        pd.read_excel(TABELAS_AUX, sheet_name = 11) \
          .to_csv(os.path.join(aux_path, 'PAIS.csv').replace('\\', '/'), header = True, index = False)

if __name__ == '__main__':
    if type(sys.argv[1]) is str:
        crawler = CrawlerComex(sys.argv[1])
        crawler.get_data()
    else:
        raise Exception("O argumento deve ser um string com o caminho de destino")