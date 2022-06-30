'''
Source code conatins all business rules.
'''
import service.mysql_service as mysql # Inner
import service.avro_service as avro # Inner
import pandas as pd
import random
import os # To define an env variable.
import service.mysql_service as mysql # Inner
import mysql.connector as sql

'''def insert_into_database(values, config):    
        output = pd.DataFrame()
        for row in values:
                byteObject = bytes(row)
                result = avro.decode(config['avro_schema'], byteObject)    
                print(result)            
                print(result.get('id'))

                new = pd.DataFrame.from_dict([result])

                # fake name will be like name_166
                new['name'] = "name_" + str(result.get('id')) 
                
                # Age will be random between 16 and 99
                new['age'] = random.randint(16,99) 

                # Account number will be id + - + random between 0 and 999
                new['account'] =  str(result.get('id')) + "-" +  str(random.randint(0,999)) 
                                
                # invented rules to set account type and customer score.
                if (result.get('account_balance') >= 3000 and result.get('account_balance') < 10000 ):
                        new['account_type'] = "Ricao"
                        new['score'] = 20
                elif (result.get('account_balance') >= 10000):
                        new['account_type'] = "Chefao"
                        new['score'] = 40
                else:              
                        new['account_type'] = "Povao"
                        new['score'] = 10

                # invented rules to set account manager and customer score.
                if (result.get('agency') == '666'):
                        new['manager'] = "Fulano"
                elif (result.get('agency')  == '555'):
                        new['manager'] = "Ciclano"
                else:              
                        new['manager'] = "Beltrano"

                output = output.append(new, ignore_index=True)
        mysql.insert(config['db_url'], config['db_table'], output)'''

def insert_into_database(values, config):    
        output = []
        for row in values:
                byteObject = bytes(row)
                result = avro.decode(config['avro_schema'], byteObject)    
                print(result)            
                print(result.get('id'))

                output.append(result)

        if len(output):
                print(type(output))
                df_message = pd.DataFrame(output)
                print(df_message.columns)
                print(df_message)

                group_ids = tuple(df_message['id'])
                print(group_ids)

                db_connection = sql.connect(host='localhost', database='impacta', user='root', password='root')

                db_cursor = db_connection.cursor()

                query = 'SELECT cod_cli,nome,idade,gerente_conta,conta_corrente,tipo_conta_corrente,score_credito FROM impacta.customer where cod_cli in ' + str(group_ids)

                print(query)

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

                print(df_sel_client)
                print(df_sel_client.columns)
                
                df_enriq = df_message.merge(
                        df_sel_client,
                        how='inner',
                        left_on='id',
                        right_on='cod_cli')
                print(df_enriq.columns)
                print(df_enriq)

                # Nova coluna
                df_enriq['decision'] = "N/A"

                # Valor do saque maior que saldo em conta
                df_decision_trans_value = df_enriq.query('type == 1 and operation_value > account_balance')[['cod_cli','decision']]
                df_decision_trans_value['decision'] = 'Operação indisponivel, valor do saque maior que saldo em conta'

                print('df_decision_trans_value')
                print(df_decision_trans_value)

                # Empréstimo pessoal
                df_decision_loan = df_enriq.query('score_credito >= 300 and tipo_conta_corrente=="Ricão"')[['cod_cli','decision']]
                df_decision_loan['decision'] = 'Campanha de empréstimo pessoal'

                print('df_decision_loan')
                print(df_decision_loan)

                # Renegociação de dívidas
                df_decision_divida = df_enriq.query('idade >= 30 or score_credito <= 200')[['cod_cli','decision']]
                df_decision_divida['decision'] = 'Renegociação de dívidas disponivel'

                print('df_decision_divida')
                print(df_decision_divida)

                # Agrupa decisões
                df_decisions = pd.concat([df_decision_trans_value, df_decision_loan, df_decision_divida])
                df_agg_decisions = df_decisions.groupby(by='cod_cli').agg(lambda decis: '/'.join(decis))
                df_agg_decisions.reset_index(inplace=True)

                print(df_agg_decisions)

                # Insere decisões
                for cod in df_agg_decisions['cod_cli']:
                        df_enriq.loc[df_enriq.cod_cli==cod,'decision'] = df_agg_decisions.loc[df_agg_decisions.cod_cli==cod,'decision']

                print('final decision')
                print(df_enriq)

                #### CONVERTER df_enriq.to_dict(orient='record') PARA JSON 
                print(df_enriq.to_json(orient='records'))
                
                #### VARRRER JSON ACIMA
                #### ENVIAR PARA NOVO TOPICO (kafka_enrichment_topic)

                ### CRIAR NOVO AVRO SCHEMA NA PASTA FILE -> PArecido com o customer.avsc -> Com campos dos enriquecimento. 
                '''
                        for i in data['customers']:
                                bytes_message = avro.encode(config['avro_schema'], i)
                                producer.send(config['kafka_enrichment_topic'], bytes_message)   
                '''

def insert_default_values(config):
        print('Inserting raw values into database.')

        df_raw_client = pd.read_csv(
                        os.path.join('file','dataset_cliente.csv'),
                        sep=';',
                        encoding='utf-8',
                        header=0)

        mysql.insert(config['db_url'], config['db_table'], df_raw_client)

