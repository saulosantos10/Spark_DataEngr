import service.yaml_service as yaml # Inner
import service.rule_service as rule # Inner
import os # To define an env variable.
import findspark
import service.mysql_service as mysql # Inner
import pandas as pd

SCALA_VERSION = '2.12'
SPARK_VERSION = '3.2.1'

os.environ["SPARK_HOME"] = "C:\spark\spark-3.2.1-bin-hadoop3.2"
os.environ["HADOOP_HOME"] = "C:\spark\spark-3.2.1-bin-hadoop3.2\hadoop"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'

print('********************************************************')
print('PYSPARK STREAMING CONSUMER STARTED')
print('********************************************************')

findspark.init()
config = yaml.read_yaml('file/config')

#### INSERT DEFAULT VALUES IN MYSQL TO ENRICH THE PROCCESS.
#rule.insert_default_values(config)

from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("realtime_processor") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config['kafka_server']) \
    .option("subscribe", config['kafka_topic']) \
    .load()

def streaming(df, batch_id):
        df.selectExpr("CAST(value AS STRING) as json")
        requests = df.rdd.map(lambda x: x.value).collect()
        rule.set_decisions(requests, config)        

query = df.writeStream \
        .format("console") \
        .foreachBatch(streaming) \
        .outputMode("append") \
        .start() \
        .awaitTermination()





