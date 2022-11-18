import requests
import os
from zipfile import ZipFile
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
import pandas as pd

#Baixando arquivo no site do INEP
url = 'http://download.inep.gov.br/microdados/microdados_enem_2021.zip'

response = requests.get(url, stream=True, verify=False)

with open('enem2021.zip', "wb") as f:
  for chunk in response.iter_content(chunk_size=512):
    if chunk: 
      f.write(chunk)

# Descompactando a base de dados
with ZipFile('/content/enem2021.zip', 'r') as  zip:
    zip.extractall(path='/content/enem/dados')

# Baixando a planilha alunos do Bucket S3
s3 = boto3.client('s3')
s3.download_file('consultoria-enem-421321341212', 'alunos.xlsx', 'alunos.xlsx')

# Instanciando o SparkSession e criando o DataFrame
spark = SparkSession.builder.appName("Enem_ETL").getOrCreate()

df = spark.read.format('csv') \
.option('header', 'true') \
.option('inferSchema', 'true') \
.option('delimiter', ';') \
.option('encoding', 'iso-8859-1') \
.load('/content/enem/dados/DADOS/MICRODADOS_ENEM_2021.csv')

df.show()

# Printando informações da Base de Dados
print('Número de linhas: {}'.format(str(df.count())))
print('Número de colunas: {}'.format(len(df.columns)))

# Selecionando as colunas que precisaremos
df_filtrado = df.select(
    'NU_INSCRICAO', 'NU_ANO', 'TP_ST_CONCLUSAO', 'TP_ANO_CONCLUIU', 'NO_MUNICIPIO_PROVA', 'TP_STATUS_REDACAO','NU_NOTA_COMP1',
    'NU_NOTA_COMP2','NU_NOTA_COMP3','NU_NOTA_COMP4','NU_NOTA_COMP5','NU_NOTA_REDACAO'
)

# Filtrando pela região de prova em Maceió
local_df = df_filtrado.filter(df.NO_MUNICIPIO_PROVA == 'Maceió')
local_df.show()

#Lendo planilha excel com Pandas
pdf = pd.read_excel('/content/alunos.xlsx')
pdf.head(5)

# Definindo schema e criando Spark DataFrame a partir do Pandas DataFrame
schema = StructType([
    StructField('NU_INSCRICAO', LongType()),
    StructField('Nome', StringType())])
df_pandas_spark = spark.createDataFrame(pdf, schema)
df_pandas_spark.show()

#Realizando o join entre os DataFrames para adicionar o nome dos participantes
df_alunos_enem = local_df.join(df_pandas_spark, on=["NU_INSCRICAO"], how="inner")
df_alunos_enem.show()

#Ordenando as colunas do nosso DataFrame
df_alunos_enem = df_alunos_enem.select('NU_INSCRICAO', 'Nome', 'NU_ANO','NO_MUNICIPIO_PROVA', 'TP_ST_CONCLUSAO','TP_ANO_CONCLUIU', 'TP_STATUS_REDACAO', 'NU_NOTA_COMP1', 'NU_NOTA_COMP2', 'NU_NOTA_COMP3', 'NU_NOTA_COMP4', 'NU_NOTA_COMP5','NU_NOTA_REDACAO')

# Verificando total de registros do df e o número de notas nulas
print('Total de registros do DataFrame: {}'.format(df_alunos_enem.count()))
print('Total de notas de redação nulas: {}'.format(df_alunos_enem.select('NU_NOTA_REDACAO').where(f.col('NU_NOTA_REDACAO').isNull()).count()))

#Top 10 melhores notas do cursinho
df_alunos_enem.select('Nome','NU_NOTA_REDACAO').orderBy(f.col('NU_NOTA_REDACAO').desc()).show(10)

# Escrevendo DataFrame em JSON e salvando no bucket S3
df_alunos_enem.write.json('/content/json', mode='overwrite')

def uploadDirectory(path,bucketname):
        for root,dirs,files in os.walk(path):
            for file in files:
              if file.endswith('.json'):
                s3.upload_file(os.path.join(root,file),bucketname,file)
uploadDirectory('/content/json', 'consultoria-enem-421321341212')