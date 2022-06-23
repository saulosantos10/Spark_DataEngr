import service.yaml_service as yaml # Inner
import service.avro_service as avro # Inner
import os # To define an env variable.

# Define a dynamic env variable for using in spark.
#os.environ["SPARK_HOME"] = "C:\spark\spark-3.3.0-bin-hadoop3"
#os.environ["HADOOP_HOME"] = "C:\spark\spark-3.3.0-bin-hadoop3\hadoop"

# https://stackoverflow.com/questions/63510654/microbatchexecution-query-terminated-with-error-unsatisfiedlinkerror-org-apach
# https://stackoverflow.com/questions/71494205/unable-to-run-pyspark-on-local-windows-environment-org-apache-hadoop-io-nativei

'''
Erro:
Exception in thread "main" java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Ljava/lang/String;I)Z

Link que ajudou:
https://stackoverflow.com/questions/41851066/exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io

O winutils.exe , hadoop.dll deve estar abaixo do C:\spark\spark-3.2.1-bin-hadoop3.2\hadoop\bin

Mas a dica chave foi:

In addition to other solutions, Please download winutil.exe and hadoop.dll and add to $HADOOP_HOME/bin. It works for me.

Depois disso o streaming funcionou!
'''

os.environ["SPARK_HOME"] = "C:\spark\spark-3.2.1-bin-hadoop3.2"
os.environ["HADOOP_HOME"] = "C:\spark\spark-3.2.1-bin-hadoop3.2\hadoop"

SCALA_VERSION = '2.12'
SPARK_VERSION = '3.2.1'

'''
Para o erro: 
pyspark.sql.utils.AnalysisException:  Failed to find data source: kafka. Please deploy the application as per the deployment section of "Structured Streaming + Kafka Integration Guide".

Erro se refere a não achar o pacote do Kafka no Spark.
Usar variável de ambiente abaixo para corrigir o erro
'''
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'

# This tool helps to find where spark is installed.
import findspark
findspark.init()

from pyspark.sql import SparkSession

print('PYSPARK STREAMING CONSUMER STARTED')

spark = SparkSession \
        .builder \
        .appName("realtime_processor") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "impacta_project") \
    .load()

## Parametro abaixo pega tudo pra tras
#.option("startingOffsets", "earliest") \

def func_call(df, batch_id):
        df.selectExpr("CAST(value AS STRING) as json")
        requests = df.rdd.map(lambda x: x.value).collect()

        for row in requests:
                # Convert the bytearray object into  bytes object
                byteObject = bytes(row)
                config = yaml.read_yaml('file/config')                
                result = avro.decode(config['avro_schema'], byteObject)
                print(result)

query = df.writeStream \
      .format("console") \
        .foreachBatch(func_call) \
      .outputMode("append") \
      .start() \
      .awaitTermination()

query.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")