import service.yaml_service as yaml # Inner
import service.avro_service as avro # Inner
import json
from kafka import KafkaProducer

config = yaml.read_yaml('file/config')
producer = KafkaProducer(bootstrap_servers=config['kafka_server'])

with open(config['input_data']) as json_file:
    data = json.load(json_file)

    for i in data['customers']:
        bytes_message = avro.encode(config['avro_schema'], i)
        producer.send(config['kafka_topic'], bytes_message)        
producer.flush()