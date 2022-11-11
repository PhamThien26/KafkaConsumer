import time
from json import loads
from kafka import KafkaConsumer

consumer = KafkaConsumer('TutorialTopic2', bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest', enable_auto_commit=True,
                         auto_commit_interval_ms=1000, group_id='consumer-group-b')

if __name__ == '__main__':
    while True:
        for message in consumer:
            message = loads(message.value.decode('utf-8'))
            print(message)
