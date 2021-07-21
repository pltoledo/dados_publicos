import requests
import os
import zipfile
from bs4 import BeautifulSoup
from src.cnpj.utils import join_path, create_dir
import sys

class CrawlerCNPJ:
    def __init__(self, save_dir) -> None:
        """
        Instancia o crawler de CNPJ.
        
        Parameters
        ----------            
        save_dir : str
            Caminho para a pasta de destino dos arquivos a serem baixados. Ela é criada caso ainda não exista.

        Returns
    	-------
        self:
            retorna uma instância do próprio objeto
        """
        self.base_url = 'http://200.152.38.155/CNPJ/'
        self.save_dir = save_dir
        create_dir(save_dir)
        r = requests.get(self.base_url)
        soup = BeautifulSoup(r.content, 'html.parser')
        self.files = [i['href'] for i in soup.select('a', href=True) 
                      if i['href'].endswith('.zip')]

    def download_url(self, url, save_path):
        """
        Função que faz o download dos dados a partir da URL.
        
        Parameters
        ----------            
        url : str
            URL de download do aqruivo 
        save_path : str
            Caminho em que o arquivos será salvo

        Returns
    	-------
        self:
            retorna uma instância do próprio objeto
        """
        r = requests.get(url)
        with open(save_path, 'wb') as fd:
            fd.write(r.content)
    
    def get_data(self, overwrite = True):
        """
        Wrapper para o download dos dados.
        
        Parameters
        ----------            
        overwrite : bool
            Indicador de que os dados devem ser sobescritos, caso já estejam baixados. Se `true`, os arquivos são sobescritos.

        Returns
    	-------
        self:
            retorna uma instância do próprio objeto
        """
        for file in self.files:
            url = self.base_url + file
            save_path = join_path(self.save_dir, file)
            if not os.path.exists(save_path):
                self.download_url(url, save_path)
            elif overwrite:
                self.download_url(url, save_path)
            else:
                continue

    def unzip(self):
        """
        Extrai os dados a partir do zip de download.
        
        Parameters
        ----------            
        Returns
    	-------
        self:
            retorna uma instância do próprio objeto
        """
        for file in self.files:   
            filepath = join_path(self.save_dir, file)
            newpath = join_path(self.save_dir, file.replace('.zip', ''))
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