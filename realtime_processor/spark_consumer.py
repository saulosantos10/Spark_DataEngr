import service.yaml_service as yaml # Inner
import service.avro_service as avro # Inner
import os # To define an env variable.
import findspark
import pandas as pd
import sqlite3

SCALA_VERSION = '2.12'
SPARK_VERSION = '3.2.1'

os.environ["SPARK_HOME"] = "C:\spark\spark-3.2.1-bin-hadoop3.2"
os.environ["HADOOP_HOME"] = "C:\spark\spark-3.2.1-bin-hadoop3.2\hadoop"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'

print('PYSPARK STREAMING CONSUMER STARTED')

findspark.init()
config = yaml.read_yaml('file/config')
dir_file = os.path.join(os.getcwd(),'file')

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

dict_message = {
 'cod_cli': [13, 234]
,'ag_cli': [1234, 3]
,'valor_op': [323.23, 213.22]
,'tipo_op': ['Deposito', 'Saque']
,'data_op': ['2020-06-12 21:00:00', '2020-05-23 21:00:00']
,'saldo_cli': ['1033.06','10.33']
}

df_raw_message = pd.DataFrame(dict_message)

dict_message_datatype = {
 'cod_cli': 'int64'
,'valor_op': 'float64'
,'data_op': 'datetime64'
,'saldo_cli': 'float64'
}
df_message = df_raw_message.astype(dict_message_datatype)

df_raw_client = pd.read_csv(
	os.path.join(dir_file,'dataset_cliente.csv'),
	sep=';',
	encoding='utf-8',
	header=0)

dict_client_datatype = {
 'cod_cli': 'int64'
,'idade': 'int64'
,'score_credito': 'int64' #  De 0 a 1000
}

df_client = df_raw_client.astype(dict_client_datatype)

# Data enrichment
conn = sqlite3.connect(os.path.join(dir_file,'enric_database.db'))
cur = conn.cursor()

# Só precisa executar uma vez, banco armazenado em arquivo
df_client.to_sql('tb_cliente', conn, index=False, if_exists='replace')

df_sel_client = pd.read_sql(f"""
SELECT *
FROM tb_cliente
where cod_cli in {tuple(df_message['cod_cli'])};
""", conn)

df_enric = df_message.merge(
	df_sel_client,
	how='inner',
	left_on='cod_cli',
	right_on='cod_cli')

# Decision making

# New column
df_enric['decision'] = "N/A"

# Valor do saque maior que saldo em conta
df_decision_trans_value = df_enric.query('tipo_op == "Saque" and valor_op > saldo_cli')[['cod_cli','decision']]
df_decision_trans_value['decision'] = 'Operação indisponivel, valor do saque maior que saldo em conta'

# Empréstimo pessoal
df_decision_loan = df_enric.query('score_credito >= 700 and tipo_conta_corrente=="Ricão"')[['cod_cli','decision']]
df_decision_loan['decision'] = 'Campanha de empréstimo pessoal'

# Renegociação de dívidas
df_decision_debt = df_enric.query('idade >= 30 or score_credito <= 400')[['cod_cli','decision']]
df_decision_debt['decision'] = 'Renegociação de dívidas disponivel'

# Agrupa decisões
df_decisions = pd.concat([df_decision_trans_value, df_decision_loan, df_decision_debt])
df_agg_decisions = df_decisions.groupby(by='cod_cli').agg(lambda decis: '/'.join(decis))
df_agg_decisions.reset_index(inplace=True)

# Insere decisões
for cod in df_agg_decisions['cod_cli']:
	df_enric.loc[df_enric.cod_cli==cod,'decision'] = df_agg_decisions.loc[df_agg_decisions.cod_cli==cod,'decision']

df_enric.to_dict(orient='record')

