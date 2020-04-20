from kafka import KafkaProducer
from kafka.errors import KafkaError
from time import sleep
from uuid import uuid4
from ipaddress import IPv4Address
import time
import random

def generateRandomUidList() :
    """Generate a list of UIDs to be used for generating displays/clicks"""
    list = [];
    for i in range(1000):
        list.append(str(uuid4()))
    return list


def generateRandomIpList():
    list = [];
    for i in range(100):
        random.seed(random.randint(1, 0xffffffff))
        list.append(str(IPv4Address(random.getrandbits(32))))
    return list

def getEvent(uid: str, ipAddress: str):
    """Generate a random click event in Json"""
    timestamp = int(time.time())
    return bytes(f'{{ "uid":"{uid}", "timestamp":{timestamp}, "ip":"{ipAddress}"}}', encoding='utf-8')

kafkaProducer = KafkaProducer(bootstrap_servers=['kafka:9093'])
msTime = 1 / 10
uidList = generateRandomUidList()
ipList = generateRandomIpList()

while True:
    clickValue = getEvent(random.choice(uidList), random.choice(ipList));
    displayValue = getEvent(random.choice(uidList), random.choice(ipList));
    kafkaProducer.send('clicks', clickValue);
    sleep(msTime);

producer.flush()
