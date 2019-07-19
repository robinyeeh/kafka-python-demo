# -*- coding: utf-8 -*-
import json

from kafka import KafkaProducer

KAFKA_HOST = '192.168.1.100'
KAFKA_PORT = 9092
KAFKA_TOPIC = 'topic_name'


class MyKafkaProducer:
    '''
    消息生产者
    '''

    def __init__(self, kafka_host, kafka_port, kafka_topic):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.kafka_topic = kafka_topic

        bootstrap_servers = '{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafka_host,
            kafka_port=self.kafka_port
        )

        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def send_message(self, data):
        target_data = json.dumps(data)
        target_data = target_data.encode('utf-8')

        self.producer.send(self.kafka_topic, value=target_data)
        self.producer.flush()


my_producer = MyKafkaProducer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC)
my_producer.send_message({'id': 123})
