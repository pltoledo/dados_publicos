from unidecode import unidecode
import pyspark.sql.functions as f
import pyspark.sql.types as t
import os
import geocoder

@f.udf(returnType = t.StringType())
def unidecode_udf(string):
    if string is None:
        return None
    else:
        return unidecode(string)

def clean_types(dtype, cols):
    def _(df):
        nonlocal cols
        cols = [cols] if type(cols) is not list else cols
        if dtype == 'int':
            for c in cols:
                    df = df.withColumn(c, f.col(c).cast('int'))
        if dtype == 'str':
            for c in cols:
                    df = df.withColumn(c, f.initcap(f.trim(unidecode_udf(f.col(c)))))
        if dtype == 'date':
            for c in cols:
                    df = df.withColumn(c, f.to_date(f.concat_ws('-', f.col(c).substr(1, 4), f.col(c).substr(5, 2), f.col(c).substr(7, 2))))
        return df
    return _

def create_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)

def join_path(*args):
    return os.path.join(*args).replace('\\', '/')

def csv_to_parquet(spark, file_path, save_path):
    spark.read.format('csv').option("header", "true").load(file_path) \
        .write.format('parquet').mode('overwrite').save(save_path)

@f.udf(returnType = t.ArrayType(t.FloatType()))
def geocoder_udf(rua, cep, bairro, municipio, uf):
    address = f'{rua}, {bairro}, {municipio}, {uf}'
    g = geocoder.arcgis(address)
    if g.address.find(municipio) > -1 and ((g.score == 100) or (g.score >= 80 and g.quality in ["PointAddress", "StreetAddress"])):
        return [g.lat, g.lng]
    else:
        address = f'{bairro}, {municipio}, {uf}'
        g = geocoder.arcgis(address)
        if g.address.find(municipio) > -1 and g.score >= 80:
            return [g.lat, g.lng]
        else:
            address = f'{cep}, {municipio}, {uf}'
            g = geocoder.arcgis(address)
            return [g.lat, g.lng]