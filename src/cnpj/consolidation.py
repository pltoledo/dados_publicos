import pyspark.sql.functions as f
import pyspark.sql.types as t
from src.cnpj.utils import unidecode_udf, create_dir, join_path, clean_types
import os
import sys
import re
import shutil


class CleanCNPJ:

    def __init__(self, spark_session, file_dir, save_dir) -> None:
        self.spark = spark_session
        self.file_dir = file_dir
        self.save_dir = save_dir
        create_dir(save_dir)
    
    def clean_tbl_aux(self):
        base_save_path = join_path(self.save_dir, 'aux_tables')
        create_dir(base_save_path)
        files = os.listdir(self.file_dir)
        aux_tables = [f for f in files 
                     if f.endswith('CSV') 
                     and re.search('(?<=\d\\.).*?(?=CSV)', f).group(0) 
                        in ['CNAE', 'MUNIC', 'NATJU', 'PAIS', 'QUALS']]
        for file in aux_tables:
            filepath = join_path(self.file_dir, file, '*')
            data_name = re.search('(?<=\d\\.).*?(?=CSV)', file).group(0).lower()
            data_name = data_name if data_name not in ['munic'] else 'mun'
            # Lê e limpa os dados
            cols = [f'cod_{data_name}', f'nome_{data_name}']
            schema = t.StructType([t.StructField(c, t.StringType()) for c in cols])
            df = (
                self.spark.read.format('csv')
                .option("encoding", "ISO-8859-1")
                .option("sep", ";")
                .load(filepath, schema = schema)
            )
            df = (
                df.withColumn(cols[1], f.trim(f.initcap(unidecode_udf(f.col(cols[1])))))
                .withColumn(cols[0], f.col(cols[0]).cast('int'))
            )
            # Salva os dados em formato .parquet
            save_path = join_path(base_save_path, f'df_{data_name}')
            df.write.format('parquet').option("encoding", "UTF-8").save(save_path)

    def clean_tbl_simples(self):
        files = os.listdir(self.file_dir)
        file = [f for f in files if f.find('SIMPLES') > -1][0]
        filepath = join_path(self.file_dir, file, '*')
        # Lê e limpa os dados
        cols = ['cnpj', 'opcao_simples', 'data_inclusao_simples','data_exclusao_simples',
                'opcao_mei', 'data_inclusao_mei', 'data_exclusao_mei']
        schema = t.StructType([t.StructField(c, t.StringType()) for c in cols])
        df = (
            self.spark.read.format('csv')
            .option("encoding", "ISO-8859-1")
            .option("sep", ";")
            .load(filepath, schema = schema)
        )
        # Limpeza básica a partir dos tipos
        date_cols = [c for c in cols if c.find("data") > -1]
        df = df.transform(clean_types('date', date_cols))
        # Salva os dados em formato .parquet
        save_path = join_path(self.save_dir, 'df_simples')
        df.write.format('parquet').option("encoding", "UTF-8").save(save_path)

    def cleaning_routines(self, table, filepath, save_path, aux_path):
        if table == 'empresas':
            # Tabelas Auxiliares
            df_natju = (
                self.spark.read.format('parquet')
                .load(join_path(aux_path, 'df_natju'))
            )
            df_quals = (
                self.spark.read.format('parquet')
                .load(join_path(aux_path, 'df_quals'))
            )
            # Tabela Principal
            cols = ['cnpj', 'razao_social', 'cod_natju', 'cod_quals', 
                    'capital_social', 'porte', 'ente_fed_resp']
            schema = t.StructType([t.StructField(c, t.StringType()) for c in cols])
            df = (
                self.spark.read.format('csv')
                .option("encoding", "ISO-8859-1")
                .option("sep", ";")
                .load(filepath, schema = schema)
            )
            # Limpeza basica a partir dos tipos
            int_cols = [c for c in cols if c.startswith('cod')] + ['porte']
            string_cols = ['razao_social', 'ente_fed_resp']
            df = (
                df.transform(clean_types('int', int_cols))
                .transform(clean_types('str', string_cols))
            )
            # Limpeza Especifica
            df = (
                df.withColumn('cnpj', f.lpad(f.col('cnpj'), 8, '0'))
                .withColumn('capital_social', f.regexp_replace(f.col('capital_social'), ',', '.').cast('float'))
                .withColumn('nome_porte', f.when(f.col('porte') == 1, 'Nao Informado')
                                           .when(f.col('porte') == 2, 'Micro Empresa')
                                           .when(f.col('porte') == 3, 'Empresa de Pequeno Porte')
                                           .when(f.col('porte') == 5, 'Demais')
                                           .otherwise(None)
                )
                .join(f.broadcast(df_natju), 'cod_natju', 'left')
                .join(f.broadcast(df_quals), 'cod_quals', 'left')
                .select('cnpj', 'razao_social', 'capital_social', 'porte', 'nome_porte', 'ente_fed_resp', 
                        'cod_natju', 'nome_natju', 'cod_quals', 'nome_quals')
            )
            df.write.format('parquet').option("encoding", "UTF-8").save(save_path)
        
        if table == 'socios':
            # Tabelas Auxiliares
            df_pais = (
                self.spark.read.format('parquet')
                .load(join_path(aux_path, 'df_pais'))
            )
            df_quals = (
                self.spark.read.format('parquet')
                .load(join_path(aux_path, 'df_quals'))
            )
            # Tabela Principal
            cols = ['cnpj_empresa', 'id_socio', 'nome_socio', 'cpf_cnpj_socio', 'cod_quals', 'data_entrada_sociedade', 
                    'cod_pais', 'num_rep_legal', 'nome_rep_legal', 'cod_quals_rep_legal', 'faixa_etaria']
            schema = t.StructType([t.StructField(c, t.StringType()) for c in cols])
            df = (
                self.spark.read.format('csv')
                .option("encoding", "ISO-8859-1")
                .option("sep", ";")
                .load(filepath, schema = schema)
            )
            # Limpeza basica a partir dos tipos
            int_cols = [c for c in cols if c.startswith('cod')] + ['id_socio', 'faixa_etaria']
            string_cols = [c for c in cols if c.startswith('nome')]
            date_cols = [c for c in cols if c.find("data") > -1]
            df = (
                df.transform(clean_types('int', int_cols))
                .transform(clean_types('str', string_cols))
                .transform(clean_types(df, 'date', date_cols))
            )
            # Limpeza Especifica
            df = (
                df.withColumn('cnpj_empresa', f.lpad(f.col('cnpj_empresa'), 8, '0'))
                .withColumn('id_socio', f.when(f.col('id_socio') == 1, 'PJ')
                                        .when(f.col('id_socio') == 2, 'PF')
                                        .when(f.col('id_socio') == 3, 'Estrangeiro')
                                        .otherwise(None))
                .withColumn('faixa_etaria', f.when(f.col('faixa_etaria') == 0, 'Nao se aplica')
                                            .when(f.col('faixa_etaria') == 1, '0 a 12 anos')
                                            .when(f.col('faixa_etaria') == 2, '13 a 20 anos')
                                            .when(f.col('faixa_etaria') == 3, '21 a 30 anos')
                                            .when(f.col('faixa_etaria') == 4, '31 a 40 anos')
                                            .when(f.col('faixa_etaria') == 5, '41 a 50 anos')
                                            .when(f.col('faixa_etaria') == 6, '51 a 60 anos')
                                            .when(f.col('faixa_etaria') == 7, '61 a 70 anos')
                                            .when(f.col('faixa_etaria') == 8, '71 a 80 anos')
                                            .when(f.col('faixa_etaria') == 9, 'Mais de 80 anos')
                                            .otherwise(None))
                .join(f.broadcast(df_quals.select(*[f.col(c).alias(c + '_rep_legal') for c in df_quals.columns])), 'cod_quals_rep_legal', 'left')
                .join(f.broadcast(df_quals), 'cod_quals', 'left')
                .join(f.broadcast(df_pais), 'cod_pais', 'left')
                .select('cnpj_empresa', 'nome_socio', 'cpf_cnpj_socio', 'id_socio', 'faixa_etaria', 
                        'cod_quals', 'nome_quals', 'data_entrada_sociedade', 'cod_pais', 'nome_pais', 
                        'num_rep_legal', 'nome_rep_legal', 'cod_quals_rep_legal', 'nome_quals_rep_legal')
            )
            df.write.format('parquet').option("encoding", "UTF-8").save(save_path)
        if table == 'estabele':
            # Tabelas Auxiliares
            df_pais = (
                self.spark.read.format('parquet')
                .load(join_path(aux_path, 'df_pais'))
            )
            df_mun = (
                self.spark.read.format('parquet')
                .load(join_path(aux_path, 'df_mun'))
            )
            # Tabela Principal
            cols = ['cnpj', 'cnpj_ordem', 'cnpj_dv', 'id_matriz', 'nome_fantasia', 
                    'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
                    'nome_cidade_ext', 'cod_pais', 'data_inicio_atividades', 
                    'cnae_primario', 'cnae_secundario', 
                    'tipo_logradouro', 'logradouro', 'numero', 'complemento', 
                    'bairro', 'cep', 'uf', 'cod_mun', 
                    'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'fax', 
                    'correio_eletronico', 'situacao_especial', 'data_situacao_especial']
            schema = t.StructType([t.StructField(c, t.StringType()) for c in cols])
            df = (
                self.spark.read.format('csv')
                .option('encoding', 'ISO-8859-1')
                .option('sep', ';')
                .option("escape", "\"")
                .load(filepath, schema = schema)
            )
            # Limpeza basica a partir dos tipos
            int_cols = [c for c in cols if c.startswith('cod')] + ['situacao_cadastral', 'motivo_situacao_cadastral']
            string_cols = [c for c in cols if c.startswith('nome') or c.find('logradouro') > -1] + ['complemento', 'bairro']
            date_cols = [c for c in cols if c.find("data") > -1]
            df =(
                df.transform(clean_types('int', int_cols))
                .transform(clean_types('str', string_cols))
                .transform(clean_types(df, 'date', date_cols))
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
                .select(*cols + ['nome_mun', 'nome_pais'])
            )
            df.write.format('parquet').option("encoding", "UTF-8").save(save_path)

    def clean_big_tbl(self, table):
        files = os.listdir(self.file_dir)
        aux_path = join_path(self.save_dir, 'aux_tables')
        int_path = join_path(self.save_dir, 'int_tables')
        create_dir(int_path)
        file_dict = {
            'empresas': 'EMPRE',
            'socios': 'SOCIO',
            'estabele': 'ESTABELE'
            }
        table_files = [f for f in files if f.find(file_dict[table]) > -1]
        # Salva em arquivos intermediários para acelerar o processamento
        int_files = []
        for file in table_files:
            filepath = join_path(self.file_dir, file)
            number = re.search('(?<=Y)\d{1}', file).group(0)
            new_name = f'df_{table}_{number}'
            save_path = join_path(int_path, new_name)
            self.cleaning_routines(table, filepath, save_path, aux_path)
            int_files.append(save_path)
        # Consolida a tabela final
        for file in int_files:
            try:
                print(f'try - {file}')
                df_final = df_final.unionByName(self.spark.read.format('parquet').load(file))
            except:
                print(f'except - {file}')
                df_final = self.spark.read.format('parquet').load(file)
        save_path = join_path(self.save_dir, f'df_{table}')
        df_final.write.format('parquet').save(save_path)
        shutil.rmtree(int_path)

    def run(self):
        self.clean_tbl_aux()
        self.clean_tbl_simples()
        self.clean_big_tbl('empresas')
        self.clean_big_tbl('socios')
        #self.clean_big_tbl('estabele')

if __name__ == "__main__":
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
                .config("spark.driver.memory", '7G') \
                .config("spark.driver.maxResultSize", '12G') \
                .config('spark.sql.adaptive.enabled', 'true') \
                .config('spark.sql.legacy.parquet.datetimeRebaseModeInWrite', 'LEGACY') \
                .getOrCreate()

    if len(sys.argv[1::]) > 0:
        cleaner = CleanCNPJ(spark, sys.argv[1], sys.argv[2])
        cleaner.run()
        spark.stop()
    else:
        raise Exception("Os argumentos devem ser strings com os caminhos de origem e destino")