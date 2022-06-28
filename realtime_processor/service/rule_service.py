'''
Source code conatins all business rules.
'''
import service.mysql_service as mysql # Inner
import service.avro_service as avro # Inner
import pandas as pd
import random

def insert_into_database(values, config):    
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
        mysql.insert(config['db_url'], config['db_table'], output)
