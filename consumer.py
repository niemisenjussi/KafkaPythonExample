from kafka import KafkaConsumer
#from pymongo import MongoClient
from json import loads
from time import sleep
import argparse

parser = argparse.ArgumentParser(description='Kafka Consumer')
parser.add_argument('clientid', type=str, help='clientid')
parser.add_argument('groupid', type=str, help='groupid')
#parser.add_argument('--sum', dest='accumulate', action='store_const',
#                    const=sum, default=max,
#                    help='sum the integers (default: find the max)')

args = parser.parse_args()

consumer = KafkaConsumer(
    'mytopic',
     client_id=args.clientid,
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset='earliest',
     enable_auto_commit=False,
     group_id=args.groupid,
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     key_deserializer=lambda x: loads(x.decode('utf-8')))

print(f"listening... {args.groupid}")
#print(f"partitions:{consumer.partitions_for_topic('mytopic')}")

#consumer.seek_to_end()

for message in consumer:
    val = message.value
    key = message.key
    #collection.insert_one(message)
    print(f"received key={key} val={val}")
    print("sleep 2")
    sleep(1)
    consumer.commit()