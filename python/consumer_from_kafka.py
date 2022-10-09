#!/usr/bin/env python
# -*- coding: utf-8 -*-

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


def main():

    schema_registry_url = 'http://localhost:8081'
    bootstrap_servers = 'localhost:9092'
    topic = 'topic-kafka-sample-v1'

    schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
    
    avro_schema = schema_registry_client.get_latest_version(f'{topic}-value')
    value_schema = avro_schema.schema.schema_str

    avro_deserializer = AvroDeserializer(value_schema, schema_registry_client)
    
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer,
        'group.id': 'media-mod-test-2',
         'auto.offset.reset': "latest",
    }

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            print(str(msg.value()))
            
        except KeyboardInterrupt:
            break
        
        except Exception as e:
            print(e)

    consumer.close()


if __name__ == '__main__':
    main()