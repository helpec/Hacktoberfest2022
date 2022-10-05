from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro import CachedSchemaRegistryClient

import time
import string
import random
import datetime


if __name__ == '__main__':


    schema_registry_url = 'http://localhost:8081'
    bootstrap_servers = 'localhost:9092'
    topic_name = 'topic-kafka-sample-v1'

    schema_registry_client = CachedSchemaRegistryClient({'url': schema_registry_url})
    avro_schema = schema_registry_client.get_latest_schema(f'{topic_name}-value')

    # avro_schema[0] Schema Id
    # avro_schema[1] Schema
    # avro_schema[2] Schema Version
    value_schema = avro_schema[1]

    all_letters = string.ascii_letters + string.digits
    length = 10

    avro_producer = AvroProducer({
        'bootstrap.servers': bootstrap_servers, 
        'schema.registry.url': schema_registry_url
    })

    def mensagem():
        date_example = datetime.datetime(
            random.randint(2000, 2020),
            random.randint(1, 12),
            random.randint(1, 28),
        )

        value = {
            "created_at": round(time.time() * 1000),
            "name": "".join(
                random.sample(all_letters,length)
            ),
            "payout_date": (
                date_example - datetime.datetime(1970,1,1)
            ).days + 1
        }

        avro_producer.produce(topic=topic_name, value=value, value_schema=value_schema)
        avro_producer.flush(1)
        
        print(value)
        print('send mensagem')


    while True:
        mensagem()
        time.sleep(5)