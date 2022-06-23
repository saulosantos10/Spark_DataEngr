import service.yaml_service as yaml # Inner
import service.avro_service as avro # Inner
import os # To define an env variable.
import findspark

SCALA_VERSION = '2.12'
SPARK_VERSION = '3.2.1'

os.environ["SPARK_HOME"] = "C:\spark\spark-3.2.1-bin-hadoop3.2"
os.environ["HADOOP_HOME"] = "C:\spark\spark-3.2.1-bin-hadoop3.2\hadoop"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'

print('PYSPARK STREAMING CONSUMER STARTED')

findspark.init()
config = yaml.read_yaml('file/config')

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

def func_call(df, batch_id):
        df.selectExpr("CAST(value AS STRING) as json")
        requests = df.rdd.map(lambda x: x.value).collect()
        for row in requests:
                byteObject = bytes(row)
                result = avro.decode(config['avro_schema'], byteObject)
                print(result)

query = df.writeStream \
        .format("console") \
        .foreachBatch(func_call) \
        .outputMode("append") \
        .start() \
        .awaitTermination()