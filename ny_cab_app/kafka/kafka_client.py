
import json
from pykafka import KafkaClient, KafkaConsumer



class ClientManager:

    def __init__(self,topic:str,host:str='127.0.0.1:9092',encoding:str='ascii'):
        self.host = host
        self.enconding = encoding
        self.topic_name = topic
        self.client = KafkaClient(hosts = self.host)
        print(self.client.topics)
        self.producer = None
        self.consumer = None
    
    def get_producer(self):
        topic= self.client.topics[self.topic_name]
        self.producer = topic.get_sync_producer()
    
    def get_consumer(self):
        consumer = KafkaConsumer('my-topic',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'])
        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                message.offset, message.key,
                                                message.value))

    def send_data(self,message:str):
        str_message = json.dumps(message)
        encode = self.enconding
        self.producer.produce(str_message.encode(encode))
