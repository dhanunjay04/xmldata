# Consumer code
import json
import pysftp as pysftp
from kafka import KafkaConsumer
import configparser
import os
from multiprocessing import Process

def find_path(json_object, tags):
    if len(tags) > 1:
        tag = tags[0]
        del tags[0]
        if json_object[tag] is not None:
            return find_path(json_object[tag], tags)
        else:
            return None
    else:
        return json_object


def find_value(json_message, path):
    tags = path.split('/')
    obj = find_path(json_message, tags)
    if obj is not None and obj[str(tags[-1])] is not None:
        return obj[str(tags[-1])]
    else:
        return None



# noinspection PyBroadException
def perform_sftp_local_to_remote(host, username, private_key, localpath, remotepath):
    try:
        srv = pysftp.Connection(host=host, username=username, private_key=private_key)
        srv.put(localpath=localpath, remotepath=remotepath)
        srv.close()
        return True
    except:
        return False


# noinspection PyBroadException
def perform_sftp_remote_to_local(host, username, private_key, localpath, remotepath):
    try:
        srv = pysftp.Connection(host=host, username=username, private_key=private_key)
        srv.get(remotepath=remotepath, localpath=localpath)
        srv.close()
        return True
    except:
        return False


def parse_notification(json_message):
    file_name = find_value(json_message, 'data/ietf-restconf:notification/nsp-nfmp:LogFileAvailableEvent/body/logFileAvailableEvent/fileName')
    server_ip = find_value(json_message, 'data/ietf-restconf:notification/nsp-nfmp:LogFileAvailableEvent/body/logFileAvailableEvent/serverIpAddress')
    if server_ip is not None and file_name is not None:
        data_set = {"file": file_name, "server": server_ip}
        return json.dumps(data_set)
    else:
        return None

def start_consumer(topicName, bootstrap_servers):
    return KafkaConsumer(topicName, group_id='group1', bootstrap_servers=bootstrap_servers)

def get_server_ip(consumer, host):
    config = configparser.ConfigParser()
    config.read(os.getcwd() + '/../kafka-config.conf')
    host1 = config['KAFKA-SERVER-PROPERTIES']['SERVER_IP']
    if not host1.__eq__(host):
        consumer.close()

def consumeMsg(ip, topicName):
    # Define server with port
    bootstrap_servers = [ip + ':9092']
    # Initialize consumer variable
    consumer = start_consumer(topicName, bootstrap_servers)
    t1 = Process(target=get_server_ip, args=(consumer, ip,))
    t1.start()
    while True:
        if not consumer.topics():
            consumer = start_consumer(topicName, bootstrap_servers)
        consumer.poll(0.1)
        for message in consumer:
            try:
                json_message = json.loads(message.value.decode())
                file_json = json.loads(parse_notification(json_message))
                if file_json is not None:
                    file_name = file_json["file"]
                    server_ip = file_json["server"]
                    print(file_name + " : " + server_ip)
            except json.JSONDecodeError:
                print("Failed to decode message from Kafka, skipping..")
            except Exception:
                print("Generic exception while pulling data points from Kafka", Exception.with_traceback())
