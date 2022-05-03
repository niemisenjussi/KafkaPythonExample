from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x:dumps(x).encode('utf-8'),
                         key_serializer=lambda x:dumps(x).encode('utf-8'),
                         )

print("producing...")
for e in range(200):
    data = {'number' : e}
    producer.send(topic='mytopic', key="firstkey", value=data)
    sleep(0.1)

producer.flush()