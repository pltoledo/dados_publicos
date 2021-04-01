import os
import sys
import re
import pyspark.sql.functions as f
import pyspark.sql.types as t

@f.udf(returnType = t.StringType())
def unidecode_udf(string):
    from unidecode import unidecode
    if string is None:
        return None
    else:
        return unidecode(string)

class CleanComex():
    
    def __init__(self, spark_session, file_dir, save_dir):
        self.spark = spark_session
        self.file_dir = file_dir
        self.save_dir = save_dir
        
        self.create_dir(save_dir)
        
    def create_dir(self, path):
        if not os.path.exists(path):
            os.mkdir(path)
    
    def get_municipios(self):
        
        """ Lê e limpa os dados de municípios
        Args:
            path (str): O caminho para os dados

        """
        
        path = os.path.join(self.file_dir, 'aux_tables/UF_MUN.csv').replace("\\","/")
        
        df = (
            self.spark.read.csv(path, header = True)
            .withColumn('nome_municipio', unidecode_udf(f.trim(f.col('NO_MUN_MIN'))))
            .withColumnRenamed('CO_MUN_GEO', 'CO_MUN')
            .select('CO_MUN', 'nome_municipio')
    )
        return df
    
    def get_pais(self):
        
        """ Lê e limpa os dados de países
        Args:
            path (str): O caminho para os dados

        """
        
        path = os.path.join(self.file_dir, 'aux_tables/PAIS.csv').replace("\\","/")
        
        df = (
            self.spark.read.csv(path, header = True)
            .withColumn('nome_pais', unidecode_udf(f.trim(f.col('NO_PAIS'))))
            .select('CO_PAIS', 'nome_pais')
        )
        return df
    
    def get_sh(self):
        
        """ Lê e limpa os dados de SH
        Args:
            path (str): O caminho para os dados

        """
        
        path = os.path.join(self.file_dir, 'aux_tables/NCM_SH.csv').replace("\\","/")
        
        df = (
            self.spark.read.csv(path, header = True)
            .withColumnRenamed('CO_SH4', 'SH4')
            .withColumnRenamed('CO_SH2', 'SH2')
            .withColumnRenamed('CO_NCM_SECROM', 'secao')
            .withColumn('nome_sh4', f.trim(f.col("NO_SH4_POR")))
            .withColumn('nome_sh2', f.trim(f.col("NO_SH2_POR")))
            .withColumn('nome_secao', f.trim(f.col("NO_SEC_POR")))
            .select('SH4', 'nome_sh4', 'SH2', 'nome_sh2', 'secao', 'nome_secao')
            .distinct()
        )
        return df
    
    def cleaning(self, path):
        
        """ Limpa os dados de importação e exportação
        Args:
            path (str): O caminho para os dados

        """
        df = self.spark.read.csv(path, header = True)
        
        # Filtra as linhas que estão todas nulas
        df = (
            df.withColumn('ind', f.when(f.coalesce(*[f.col(c) for c in df.columns]).isNull(), 1).otherwise(0))
            .filter('ind = 0')
            .drop('ind')
        )

        # Transforma as colunas de código para inteiros
        int_cols = [c for c in df.columns if c != 'SG_UF_MUN']
        for c in int_cols:
            df = df.withColumn(c, f.col(c).cast('int'))
            
        # Le os outros DFs
        df_mun = self.get_municipios()
        df_pais = self.get_pais()
        df_ncm_sh = self.get_sh()
        
        # Junta os dados
        
        final_df =  (
            df.join(df_mun, 'CO_MUN')
            .join(df_pais, 'CO_PAIS')
            .join(df_ncm_sh, 'SH4')
            .withColumnRenamed('CO_MUN', 'cod_municipio')
            .withColumnRenamed('CO_PAIS', 'cod_pais')
            .withColumnRenamed('SG_UF_MUN', 'uf')
            .withColumnRenamed('CO_ANO', 'ano')
            .withColumnRenamed('CO_MES', 'mes')
            .select('ano', 'mes', 'cod_municipio', 'nome_municipio', 'uf', 'cod_pais', 'nome_pais', 
                        'SH4', 'nome_sh4', 'SH2', 'nome_sh2', 'secao', 'nome_secao', 
                        'KG_LIQUIDO', 'VL_FOB')
        )
        
        return final_df

    def write(self, df, save_path, format='parquet'):
        if format == 'parquet':
            df.write.parquet(save_path, mode = 'overwrite')
        elif format == 'csv':
            df.toPandas().to_csv(save_path, header = True, index = False)
        else:
            raise Exception('Escolha um formato entre csv e parquet')
    
    def run(self):
        data_path = os.path.join(self.file_dir, 'mun').replace("\\","/")
        files = os.listdir(data_path)
        exp_files = [f for f in files if re.search('EXP', f)]
        imp_files = [f for f in files if re.search('IMP', f)]
        for l, name in zip([exp_files, imp_files], ['exp', 'imp']):
            for file in l:
                path = os.path.join(data_path, file).replace("\\","/")
                try:
                    df = df.unionByName(self.cleaning(path))
                except:
                    df = self.cleaning(path)
            save_path = os.path.join(self.save_dir, f'comex_{name}').replace("\\","/")
            self.write(df, save_path)
            #save_path_csv = os.path.join(self.save_dir, f'comex_{name}.csv').replace("\\","/")
            #df = self.spark.read.parquet(save_path)
            #self.write(df, save_path_csv, format='csv')

if __name__ == '__main__':
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

    if type(sys.argv[1]) is str and type(sys.argv[2]) is str:
        cleaner = CleanComex(spark, sys.argv[1], sys.argv[2])
        cleaner.run()
    else:
        raise Exception("Os argumentos devem ser strings com os caminhos de origem e destino")