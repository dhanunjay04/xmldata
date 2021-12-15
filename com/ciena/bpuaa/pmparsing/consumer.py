# Consumer code
import json
from kafka import KafkaConsumer


def parse_notification(json_message):
    if json_message is not None:
        if json_message['data'] is not None:
            data = json_message['data']
            if data['ietf-restconf:notification'] is not None:
                ietf = data['ietf-restconf:notification']
                if ietf['nsp-nfmp:LogFileAvailableEvent'] is not None:
                    log_notif = ietf['nsp-nfmp:LogFileAvailableEvent']
                    if log_notif['header'] is not None:
                        print(log_notif)


def consumeMsg(topicName):
    print("Started consumer")
    # Define server with port
    bootstrap_servers = ['localhost:9092']
    # Initialize consumer variable
    consumer = KafkaConsumer(topicName, group_id='group1', bootstrap_servers=bootstrap_servers)
    while True:
        consumer.poll(0.1)
        for message in consumer:
            try:
                json_message = json.loads(message.value.decode())
                parse_notification(json_message)
            except json.JSONDecodeError:
                print("Failed to decode message from Kafka, skipping..")
            except:
                print("Generic exception while pulling data points from Kafka")
