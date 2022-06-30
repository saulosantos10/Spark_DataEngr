import service.yaml_service as yaml # Inner
import service.avro_service as avro # Inner
from kafka import KafkaConsumer

config = yaml.read_yaml('file/config')

consumer = KafkaConsumer(config['kafka_enrichment_topic'])
for message in consumer:
    result = avro.decode(config['avro_enrichment'], message.value)
    print('********************************************************')
    print(result)