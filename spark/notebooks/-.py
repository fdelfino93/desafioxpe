#!/usr/bin/env python
# coding: utf-8

# In[14]:


pip install "flask<2.3,>=2.2"


# In[15]:


pip install python-dotenv


# In[16]:


pip install pyspark boto3


# In[17]:


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, avg, count
from dotenv import load_dotenv

load_dotenv()

spark = (SparkSession.builder
    .appName('S3 Access Example')
    .master('local[*]')
    .config('spark.jars', '/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true')
    .getOrCreate())

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION', 'us-east-2')

spark._jsc.hadoopConfiguration().set('fs.s3a.access.key', aws_access_key)
spark._jsc.hadoopConfiguration().set('fs.s3a.secret.key', aws_secret_key)
spark._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3.{aws_region}.amazonaws.com')

print('Spark session ready')


# In[18]:


# -----------------------
# 1. Carregar Variáveis de Ambiente do .env_kafka_connect
# -----------------------
load_dotenv('.env_kafka_connect')  # Carregar o arquivo .env

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = "us-east-1"


# In[19]:


# -----------------------
# 2. Inicializar a Spark Session com Configuração S3
# -----------------------

# Caminho local dos arquivos JAR no mesmo diretório do notebook
# Obter caminho absoluto dos JARs
current_dir = os.getcwd()
hadoop_aws_jar = os.path.join(current_dir, "hadoop-aws-3.3.4.jar")
aws_sdk_jar = os.path.join(current_dir, "aws-java-sdk-bundle-1.12.262.jar")

jars_path = f"{hadoop_aws_jar},{aws_sdk_jar}"

spark = SparkSession.builder \
    .appName("ETL Pipeline - S3 Integration") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", jars_path) \
    .getOrCreate()


# Configuração dinâmica das credenciais AWS no Spark
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")

# Configuração do acesso ao S3
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")


# In[20]:


# -----------------------
# 3. Ler os Dados Brutos - Camada Bronze
# não se esqueça de alterar o nome do seu bucket
# -----------------------

bronze_path = "s3a://my-bucket-ric-01/raw-data/ipca/kafka/"

# Ler os dados do S3 e testar a conexão
try:
    df_bronze = spark.read.json(bronze_path)
    print("Leitura bem-sucedida!")
    df_bronze.show()
except Exception as e:
    print(f"Erro ao acessar o S3: {e}")


# In[ ]:


# -----------------------
# 4. Tratamento dos Dados - Camada Silver
# -----------------------
# Remover duplicações e converter timestamps para datas legíveis
df_silver = df_bronze.dropDuplicates()

# Tratar timestamps e converter para formato legível
df_silver = df_silver.withColumn("Data_Vencimento", from_unixtime(col("Data_Vencimento") / 1000, "yyyy-MM-dd")) \
                     .withColumn("Data_Base", from_unixtime(col("Data_Base") / 1000, "yyyy-MM-dd")) \
                     .withColumn("dt_update", from_unixtime(col("dt_update") / 1000, "yyyy-MM-dd HH:mm:ss"))

# Tratar valores nulos
df_silver = df_silver.fillna({
    "PUCompraManha": 0,
    "PUVendaManha": 0,
    "PUBaseManha": 0
})

# Visualizar os dados transformados
print("Dados Transformados (Silver):")
df_silver.show(truncate=False)

# Salvar os dados limpos no S3 em formato Parquet
silver_path = "s3a://my-bucket-ric-01/processed-data/ipca/silver/"
df_silver.write.mode("overwrite").parquet(silver_path)


# In[ ]:


# -----------------------
# 5. Agregação e Métricas - Camada Gold
# -----------------------
# Calcular métricas agregadas
df_gold = df_silver.groupBy("Tipo").agg(
    avg("PUCompraManha").alias("Media_PUCompraManha"),
    avg("PUVendaManha").alias("Media_PUVendaManha"),
    count("*").alias("Total_Registros")
)

# Visualizar as métricas agregadas
print("Dados Agregados (Gold):")
df_gold.show(truncate=False)

# Salvar os dados agregados no S3 em formato Parquet
gold_path = "s3a://my-bucket-ric-01/analytics/ipca/gold/"
df_gold.write.mode("overwrite").parquet(gold_path)


# In[ ]:


# -----------------------
# 6. Encerrar a Spark Session
# -----------------------
spark.stop()

