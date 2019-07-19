# -*- coding: utf-8 -*-
import json

from kafka import KafkaProducer, KafkaConsumer

KAFKA_HOST = '192.168.1.100'
KAFKA_PORT = 9092
KAFKA_TOPIC = 'topic_name'


class MyKafkaConsumer:
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

        self.consumer = KafkaConsumer(self.kafka_topic, group_id='g1', bootstrap_servers=bootstrap_servers)

    def consume_message(self):
        for message in self.consumer:
            print(message)


my_consumer = MyKafkaConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC)
my_consumer.consume_message()
