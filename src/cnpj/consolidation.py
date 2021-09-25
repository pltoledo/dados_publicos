import sys
import os
sys.path.append(os.path.abspath(""))
import pyspark.sql.functions as f
from src.util import *
import os
import sys
import shutil


class CleanerCNPJ:

    def __init__(self, spark_session, file_dir, save_dir) -> None:

        self.spark = spark_session
        self.file_dir = file_dir
        self.save_dir = save_dir
        create_dir(save_dir)
        self.files = os.listdir(file_dir)
    
    def clean_aux_tables(self):

        file_ids = ['CNAE', 'MUNIC', 'NATJU', 'PAIS', 'QUALS', 'MOTI']
        aux_tables = [f for n in file_ids for f in self.files if f.endswith(n + 'CSV')]
        for file, id in zip(aux_tables, file_ids):
            file_path = join_path(self.file_dir, file)
            data_name = id.lower()
            data_name = data_name if data_name != 'munic' else 'mun'
            # Lê e limpa os dados
            cols = [f'cod_{data_name}', f'nome_{data_name}']
            schema = ', '.join([c + ' STRING' for c in cols])
            df = (
                self.spark.read.format('csv')
                .option("encoding", "ISO-8859-1")
                .option("sep", ";")
                .schema(schema)
                .load(file_path)
            )
            df = (
                df.withColumn(cols[1], f.trim(f.initcap(unidecode_udf(f.col(cols[1])))))
                .withColumn(cols[0], f.col(cols[0]).cast('int'))
            )
            # Salva os dados em formato .parquet
            save_path = join_path(self.save_dir, f'df_{data_name}')
            df.write.format('parquet').option("encoding", "UTF-8").save(save_path)

    def clean_simples(self):

        file = [f for f in self.files if f.find('SIMPLES') != -1][0]
        file_path = join_path(self.file_dir, file)
        # Lê e limpa os dados
        cols = ['cnpj', 'opcao_simples', 'data_inclusao_simples','data_exclusao_simples',
                'opcao_mei', 'data_inclusao_mei', 'data_exclusao_mei']
        schema = ', '.join([c + ' STRING' for c in cols])
        df = (
            self.spark.read.format('csv')
            .option("encoding", "ISO-8859-1")
            .option("sep", ";")
            .schema(schema)
            .load(file_path)
        )
        # Limpeza básica a partir dos tipos
        date_cols = [c for c in cols if c.find("data") != -1]
        df = df.transform(clean_types('date', date_cols))
        # Salva os dados em formato .parquet
        save_path = join_path(self.save_dir, 'df_simples')
        df.write.format('parquet').option("encoding", "UTF-8").save(save_path)

    def write_int_data(self):

        self.int_dir = join_path(self.save_dir, 'int_tables')
        create_dir(self.int_dir)
        file_names = ['EMPRE', 'SOCIO', 'ESTABELE']
        # Salva em arquivos intermediários para acelerar o processamento
        read_options = {
            'encoding': 'ISO-8859-1',
            'sep': ';',
        }
        for file in file_names:
            file_path = join_path(self.file_dir, '*', f'{file}CSV')
            if file == 'ESTABELE':
                read_options['escape'] =  "\""
            int_df = (
                self.spark.read
                .format('csv')
                .options(**read_options)
                .load(file_path)
            )
            save_path = join_path(self.int_dir, f'df_{file.lower()}_int')
            int_df.write.option("encoding", "UTF-8").format('parquet').save(save_path)

    def clean_empresas(self):

        file_path = join_path(self.int_dir, f'df_empre_int')
        # Tabela Principal
        cols = ['cnpj', 'razao_social', 'cod_natju', 'cod_quals', 
                'capital_social', 'porte', 'ente_fed_resp']
        schema = ', '.join([c + ' STRING' for c in cols])
        df = self.spark.read.schema(schema).load(file_path)
        # Tabelas Auxiliares
        df_natju = self.spark.read.format('parquet').load(join_path(self.save_dir, 'df_natju'))
        df_quals = self.spark.read.format('parquet').load(join_path(self.save_dir, 'df_quals'))
        # Limpeza basica a partir dos tipos
        int_cols = [c for c in cols if c.startswith('cod')] + ['porte']
        string_cols = ['razao_social', 'ente_fed_resp']
        df = (
            df.transform(clean_types('int', int_cols))
            .transform(clean_types('str', string_cols))
        )
        # Limpeza Especifica
        predicado = """
                    CASE WHEN porte = 1 THEN "Nao Informado"
                            WHEN porte = 2 THEN "Micro Empresa"
                            WHEN porte = 3 THEN "Empresa de Pequeno Porte"
                            WHEN porte = 5 THEN "Demais"
                            ELSE null
                    END
                    """
        df = (
            df.withColumn('cnpj', f.lpad(f.col('cnpj'), 8, '0'))
            .withColumn('capital_social', f.regexp_replace(f.col('capital_social'), ',', '.').cast('float'))
            .withColumn('nome_porte', f.expr(predicado))
            .join(f.broadcast(df_natju), 'cod_natju', 'left')
            .join(f.broadcast(df_quals), 'cod_quals', 'left')
            .select(
                'cnpj', 
                'razao_social', 
                'capital_social', 
                'porte', 'nome_porte', 
                'ente_fed_resp',
                'cod_natju', 
                'nome_natju', 
                'cod_quals', 
                'nome_quals'
            )
        )
        save_path = join_path(self.save_dir, 'df_empresas')
        df.write.format('parquet').save(save_path)

    def clean_socios(self):

        file_path = join_path(self.int_dir, f'df_socio_int')
        # Tabela Principal
        cols = ['cnpj_empresa', 'id_socio', 'nome_socio', 'cpf_cnpj_socio', 'cod_quals', 'data_entrada_sociedade', 
                    'cod_pais', 'num_rep_legal', 'nome_rep_legal', 'cod_quals_rep_legal', 'faixa_etaria']
        schema = ', '.join([c + ' STRING' for c in cols])
        df = self.spark.read.schema(schema).load(file_path)
        # Tabelas Auxiliares
        df_pais = self.spark.read.format('parquet').load(join_path(self.save_dir, 'df_pais'))
        df_quals = self.spark.read.format('parquet').load(join_path(self.save_dir, 'df_quals'))
        # Limpeza basica a partir dos tipos
        int_cols = [c for c in cols if c.startswith('cod')] + ['id_socio', 'faixa_etaria']
        string_cols = [c for c in cols if c.startswith('nome')]
        date_cols = [c for c in cols if c.find("data") != -1]
        df = (
            df.transform(clean_types('int', int_cols))
            .transform(clean_types('str', string_cols))
            .transform(clean_types('date', date_cols))
        )
        # Limpeza Especifica
        rename_quals = [f.col(c).alias(c + '_rep_legal') for c in df_quals.columns]
        predicado = """
                    CASE WHEN porte = 0 THEN "Nao se aplica"
                            WHEN porte = 1 THEN "0 a 12 anos"
                            WHEN porte = 2 THEN "13 a 20 anos"
                            WHEN porte = 3 THEN "21 a 30 anos"
                            WHEN porte = 4 THEN "31 a 40 anos"
                            WHEN porte = 5 THEN "41 a 50 anos"
                            WHEN porte = 6 THEN "51 a 60 anos"
                            WHEN porte = 7 THEN "61 a 70 anos"
                            WHEN porte = 8 THEN "71 a 80 anos"
                            WHEN porte = 9 THEN "Mais de 80 anos"
                            ELSE null
                    END
                    """
        df = (
            df.withColumn('cnpj_empresa', f.lpad(f.col('cnpj_empresa'), 8, '0'))
            .withColumn('id_socio', f.when(f.col('id_socio') == 1, 'PJ')
                                     .when(f.col('id_socio') == 2, 'PF')
                                     .when(f.col('id_socio') == 3, 'Estrangeiro')
                                     .otherwise(None))
            .withColumn('faixa_etaria', f.expr(predicado))
            .join(f.broadcast(df_quals.select(rename_quals)), 'cod_quals_rep_legal', 'left')
            .join(f.broadcast(df_quals), 'cod_quals', 'left')
            .join(f.broadcast(df_pais), 'cod_pais', 'left')
            .select(
                'cnpj_empresa', 
                'nome_socio', 
                'cpf_cnpj_socio', 
                'id_socio', 
                'faixa_etaria',
                'cod_quals', 
                'nome_quals', 
                'data_entrada_sociedade', 
                'cod_pais', 
                'nome_pais',
                'num_rep_legal',
                'nome_rep_legal', 
                'cod_quals_rep_legal', 
                'nome_quals_rep_legal'
            )
        )
        save_path = join_path(self.save_dir, 'df_socios')
        df.write.format('parquet').save(save_path)

    def clean_estab(self):
        
        file_path = join_path(self.int_dir, f'df_estabele_int')
        # Tabela Principal
        cols = ['cnpj', 'cnpj_ordem', 'cnpj_dv', 'id_matriz', 'nome_fantasia', 
                'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
                'nome_cidade_ext', 'cod_pais', 'data_inicio_atividades', 
                'cnae_primario', 'cnae_secundario', 
                'tipo_logradouro', 'logradouro', 'numero', 'complemento', 
                'bairro', 'cep', 'uf', 'cod_mun', 
                'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'fax', 
                'correio_eletronico', 'situacao_especial', 'data_situacao_especial']
        schema = ', '.join([c + ' STRING' for c in cols])
        df = self.spark.read.schema(schema).load(file_path)
        # Tabelas Auxiliares
        df_pais = self.spark.read.format('parquet').load(join_path(self.save_dir, 'df_pais'))
        df_mun = self.spark.read.format('parquet').load(join_path(self.save_dir, 'df_mun'))
        # Limpeza basica a partir dos tipos
        int_cols = [c for c in cols if c.startswith('cod')] + ['situacao_cadastral', 'motivo_situacao_cadastral']
        string_cols = [c for c in cols if c.startswith('nome')] + ['tipo_logradouro', 'logradouro', 'complemento', 'bairro']
        date_cols = [c for c in cols if c.find("data") != -1]
        df =(
            df.transform(clean_types('int', int_cols))
            .transform(clean_types('str', string_cols))
            .transform(clean_types('date', date_cols))
        )
        # Limpeza Especifica
        df = (
            df.withColumn('cnpj', f.lpad(f.col('cnpj'), 8, '0'))
            .withColumn('cnae_primario', f.lpad(f.col('cnae_primario'), 7, '0'))
            .withColumn('id_matriz', f.when(f.col('id_matriz') == 1, 'Matriz')
                                      .when(f.col('id_matriz') == 2, 'Filial')
                                      .otherwise(None))
            .withColumn('uf', f.trim(f.upper(f.col('uf'))))
            .withColumn('correio_eletronico', f.regexp_replace(f.trim(f.lower(f.col('correio_eletronico'))), "'", "@"))
            .withColumn('tipo_logradouro', f.regexp_replace(f.col('tipo_logradouro'), "ç", "c"))
            .withColumn('numero', f.when(f.col('numero').isin('S/N', 'S/N B'), '').otherwise(f.col('numero')))
            .join(f.broadcast(df_mun), 'cod_mun')
            .join(f.broadcast(df_pais), 'cod_pais', 'left')
            .select(*cols, *['nome_mun', 'nome_pais'])
        )
        save_path = join_path(self.save_dir, 'df_estab')
        df.write.format('parquet').save(save_path)

    def run(self):

        self.clean_aux_tables()
        self.clean_simples()
        self.write_int_data()
        self.clean_empresas()
        self.clean_socios()
        self.clean_estab()
        shutil.rmtree(self.int_dir)

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession \
                .builder \
                .config("spark.sql.broadcastTimeout", "360000") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
                .config('spark.sql.execution.arrow.pyspark.enabled', 'false') \
                .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")\
                .config("spark.driver.memory", '7G') \
                .config("spark.driver.maxResultSize", '12G') \
                .config('spark.sql.adaptive.enabled', 'true') \
                .config('spark.sql.legacy.parquet.datetimeRebaseModeInWrite', 'LEGACY') \
                .getOrCreate()

    if len(sys.argv[1::]) > 0:
        cleaner = CleanerCNPJ(spark, sys.argv[1], sys.argv[2])
        cleaner.run()
        spark.stop()
    else:
        raise Exception("Os argumentos devem ser strings com os caminhos de origem e destino")