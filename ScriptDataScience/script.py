import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.2.1-bin-hadoop2.7"
import findspark
findspark.init()
from google.colab import drive
from pyspark.sql import SparkSession
import zipfile
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql import functions as f


# Função para montar o seu google drive
drive.mount('/content/drive')

# Unzip dos arquivos csv que contem dados retirados da receita federal.
zipfile.ZipFile('/content/drive/MyDrive/curso-spark/socios (1).zip', 'r').extractall('/content/drive/MyDrive/curso-spark')
zipfile.ZipFile('/content/drive/MyDrive/curso-spark/empresas (1).zip', 'r').extractall('/content/drive/MyDrive/curso-spark')
zipfile.ZipFile('/content/drive/MyDrive/curso-spark/estabelecimentos (1).zip', 'r').extractall('/content/drive/MyDrive/curso-spark')

# Iniciando a sessão spark
spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .config('spark.ui.port', '4050') \
    .getOrCreate()

# Bloco para ler os arquivos csv do diretorio, separando coluna por ; e pegando a tipagem dos dados atraves do inferSchema e criando os dataFrames
empresas = spark.read.csv('/content/drive/MyDrive/curso-spark/empresas', sep=';', inferSchema=True)
estabelecimento = spark.read.csv('/content/drive/MyDrive/curso-spark/estabelecimentos', sep=';', inferSchema=True)
socios = spark.read.csv('/content/drive/MyDrive/curso-spark/socios', sep=';', inferSchema=True)
# Fim bloco de leitura e criação de dataFrame

# Bloco para renomear as colunas que por padrão vem com _c{index da coluna de forma decrescente}
empresasColNames = ['cnpj_basico', 'razao_social_nome_empresarial',
                    'natureza_juridica', 
                    'qualificacao_do_responsavel', 
                    'capital_social_da_empresa', 
                    'porte_da_empresa', 
                    'ente_federativo_responsavel']

sociosColNames = ['cnpj_basico', 
                  'identificador_de_socio', 
                  'nome_do_socio_ou_razao_social', 
                  'cnpj_ou_cpf_do_socio', 
                  'qualificacao_do_socio', 
                  'data_de_entrada_sociedade', 
                  'pais', 
                  'representante_legal', 
                  'nome_do_representante', 
                  'qualificacao_do_representante_legal', 
                  'faixa_etaria']

estabsColNames = ['cnpj_basico', 
                  'cnpj_ordem', 
                  'cnpj_dv', 
                  'identificador_matriz_filial', 
                  'nome_fantasia', 
                  'situacao_cadastral', 
                  'data_situacao_cadastral', 
                  'motivo_situacao_cadastral', 
                  'nome_da_cidade_no_exterior', 
                  'pais', 
                  'data_de_inicio_atividade', 
                  'cnae_fiscal_principal', 
                  'cnae_fiscal_secundaria', 
                  'tipo_de_logradouro', 
                  'logradouro', 
                  'numero', 
                  'complemento', 
                  'bairro', 
                  'cep', 
                  'uf', 
                  'municipio', 
                  'ddd_1', 
                  'telefone_1', 
                  'ddd_2', 
                  'telefone_2', 
                  'ddd_do_fax', 
                  'fax', 
                  'correio_eletronico', 
                  'situacao_especial', 
                  'data_da_situacao_especial']


for index, name in enumerate(empresasColNames):
  empresas =  empresas.withColumnRenamed(f"_c{index}", name)
empresas.columns

for index, colName in enumerate(sociosColNames):
    socios = socios.withColumnRenamed(f"_c{index}", colName)
socios.columns

for index, colName in enumerate(estabsColNames):
    estabelecimento = estabelecimento.withColumnRenamed(f"_c{index}", colName)
estabelecimento.columns
# Fim bloco de rename


# Bloco para tratamento dos dados usando cast e replace
empresas = empresas.withColumn('capital_social_da_empresa', f.regexp_replace('capital_social_da_empresa', ',', '.'))
empresas = empresas.withColumn('capital_social_da_empresa', empresas['capital_social_da_empresa'].cast(DoubleType()))
socios = socios.withColumn("data_de_entrada_sociedade", f.to_date(socios.data_de_entrada_sociedade.cast(StringType()), 'yyyyMMdd'))

estabelecimento = estabelecimento\
    .withColumn("data_situacao_cadastral", f.to_date(estabelecimento.data_situacao_cadastral.cast(StringType()), "yyyyMMdd"))\
    .withColumn("data_da_situacao_especial", f.to_date(estabelecimento.data_da_situacao_especial.cast(StringType()), "yyyyMMdd"))\
    .withColumn("data_de_inicio_atividade", f.to_date(estabelecimento.data_de_inicio_atividade.cast(StringType()), "yyyyMMdd"))
# Fim bloco de tratamento

# Bloco para iniciar as consultar usando sql
estabelecimento.select('*').show(5, False)
empresas.select('*').show(5, False)
empresas.select('razao_social_nome_empresarial', 'cnpj_basico').show(5, False)

socios.select('cnpj_ou_cpf_do_socio','nome_do_socio_ou_razao_social', f.year('data_de_entrada_sociedade').alias('anoEntrada')).show(5,False)

socios.select('cnpj_ou_cpf_do_socio','nome_do_socio_ou_razao_social',\
              f.year('data_de_entrada_sociedade').alias('anoEntrada'))\
              .orderBy(['anoEntrada', 'nome_do_socio_ou_razao_social'], ascending=[False,True])\
              .show(5,False)
# Fim bloco