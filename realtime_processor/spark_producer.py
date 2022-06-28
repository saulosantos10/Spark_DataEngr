import service.yaml_service as yaml # Inner
import service.mysql_service as mysql # Inner
import os # To define an env variable.
import findspark
import pandas as pd

SCALA_VERSION = '2.12'
SPARK_VERSION = '3.2.1'

os.environ["SPARK_HOME"] = "C:\spark\spark-3.2.1-bin-hadoop3.2"
os.environ["HADOOP_HOME"] = "C:\spark\spark-3.2.1-bin-hadoop3.2\hadoop"
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'

print('********************************************************')
print('PYSPARK STREAMING PRODUCER STARTED')
print('********************************************************')

findspark.init()
config = yaml.read_yaml('file/config')
dir_file = os.path.join(os.getcwd(),'file')

from pyspark.sql import SparkSession

query = f"""
SELECT *
FROM  {config['db_table']} ;
"""

df_enric=mysql.query(config['db_url'], query)

# New column
df_enric['decision'] = "N/A"

# Withdraw value higher than account balance.
df_decision_trans_value = df_enric.query('type == 1 and operation_value > account_balance')[['id','decision']]
df_decision_trans_value['decision'] = 'Unavailable Operation, withdraw value higher than account balance.'

# Empréstimo pessoal
df_decision_loan = df_enric.query('score >= 30 and account_type =="Ricao"')[['id','decision']]
df_decision_loan['decision'] = 'Campanha de empréstimo pessoal'

# Renegociação de dívidas
df_decision_debt = df_enric.query('age >= 30 or score <= 10')[['id','decision']]
df_decision_debt['decision'] = 'Renegociação de dívidas disponivel'

# Agrupa decisões
df_decisions = pd.concat([df_decision_trans_value, df_decision_loan, df_decision_debt])
df_agg_decisions = df_decisions.groupby(by='id').agg(lambda decis: '/'.join(decis))
df_agg_decisions.reset_index(inplace=True)

# Insere decisões
for cod in df_agg_decisions['id']:
	df_enric.loc[df_enric.id==cod,'decision'] = df_agg_decisions.loc[df_agg_decisions.id==cod,'decision']

print(df_enric.columns)
print(df_enric.shape)
print(df_enric)

mysql.insert(config['db_url'], config['db_rich_table'], df_enric)

