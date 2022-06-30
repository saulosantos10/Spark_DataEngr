'''
Source code contains all business rules.
'''
import service.mysql_service as mysql # Inner
import service.avro_service as avro # Inner
import pandas as pd
import random
import os # To define an env variable.
import service.mysql_service as mysql # Inner
import mysql.connector as sql
from kafka import KafkaProducer

def set_decisions(values, config):    
        producer = KafkaProducer(bootstrap_servers=config['kafka_server'])

        output = []
        for row in values:
                byteObject = bytes(row)
                result = avro.decode(config['avro_schema'], byteObject)    
                output.append(result)

        if len(output):
                df_message = pd.DataFrame(output)
                group_ids = tuple(df_message['id'])
                db_connection = sql.connect(host='localhost', database='impacta', user='root', password='root')
                db_cursor = db_connection.cursor()
                query = 'SELECT cod_cli,nome,idade,gerente_conta,conta_corrente,tipo_conta_corrente,score_credito FROM impacta.customer where cod_cli in ' + str(group_ids)
                db_cursor.execute(query)
                table_rows = db_cursor.fetchall()
                df_sel_client = pd.DataFrame(table_rows)
                dict = {0: 'cod_cli',
                        1: 'nome',
                        2: 'idade',
                        3: 'gerente_conta',
                        4: 'conta_corrente',
                        5: 'tipo_conta_corrente',
                        6: 'score_credito'}

                # call rename () method
                df_sel_client.rename(columns=dict,inplace=True)
                df_enriq = df_message.merge(
                        df_sel_client,
                        how='inner',
                        left_on='id',
                        right_on='cod_cli')

                # Nova coluna
                df_enriq['decision'] = "N/A"

                # Valor do saque maior que saldo em conta
                df_decision_trans_value = df_enriq.query('type == 1 and operation_value > account_balance')[['cod_cli','decision']]
                df_decision_trans_value['decision'] = 'Operação indisponivel, valor do saque maior que saldo em conta'

                # Empréstimo pessoal
                df_decision_loan = df_enriq.query('score_credito >= 300 and tipo_conta_corrente=="Ricão"')[['cod_cli','decision']]
                df_decision_loan['decision'] = 'Campanha de empréstimo pessoal'

                # Renegociação de dívidas
                df_decision_divida = df_enriq.query('idade >= 30 or score_credito <= 200')[['cod_cli','decision']]
                df_decision_divida['decision'] = 'Renegociação de dívidas disponivel'

                # Agrupa decisões
                df_decisions = pd.concat([df_decision_trans_value, df_decision_loan, df_decision_divida])
                df_agg_decisions = df_decisions.groupby(by='cod_cli').agg(lambda decis: '/'.join(decis))
                df_agg_decisions.reset_index(inplace=True)

                # Insere decisões
                for cod in df_agg_decisions['cod_cli']:
                        df_enriq.loc[df_enriq.cod_cli==cod,'decision'] = df_agg_decisions.loc[df_agg_decisions.cod_cli==cod,'decision']

                data = df_enriq.to_dict('records')

                print('********************************************************')
                print('FINAL DECISION')
                print('********************************************************')
                print(data)

                for i in data:
                        bytes_message = avro.encode(config['avro_enrichment'], i)
                        producer.send(config['kafka_enrichment_topic'], bytes_message)

def insert_default_values(config):
        print('Inserting raw values into database.')

        df_raw_client = pd.read_csv(
                        os.path.join('file','dataset_cliente.csv'),
                        sep=';',
                        encoding='utf-8',
                        header=0)

        mysql.insert(config['db_url'], config['db_table'], df_raw_client)
