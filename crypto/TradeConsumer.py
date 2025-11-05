import json
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from datetime import datetime

class TradeConsumer(Consumer):
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = '',value_deserializer=lambda m: json.loads(m.decode('utf-8'))):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        
        #self.consumer = Consumer(self.conf)
        self.keep_runnning = True
        self.group_id = group_id
        self.value_deserializer = value_deserializer

    def consume(self, topics, processing_func):
        try:
            self.subscribe(topics)
            while self.keep_runnning:
                msg = self.poll(timeout=1.0)
                if msg is None:
                    print("No msg to consume...")
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    print(f'processing from {self.group_id}')
                    processing_func(msg)
        finally:
            # Close down consumer to commit final offsets.
            self.close()
            
    def writeDB(self,msg):
        crypto = self.value_deserializer(msg.value())
        print(crypto)
        try:
            conn = psycopg2.connect(
                host="localhost", #change to db if running as a container
                database="postgres", 
                user="postgres",
                port = '5432',
                password="postgres")
            
            insert_query = f"""
            INSERT INTO team1.{self.group_id} (retrieve_time, price_usd, change_percent)
            VALUES (%s, %s, %s);
            """
            #has to convert string datetime back into datetime
            data = (datetime.fromisoformat(crypto['time']), crypto['priceUsd'], crypto['changePercent24Hr'])
            cur = conn.cursor()
            cur.execute(insert_query, data)
            conn.commit()
            cur.close()
            conn.close()
        except Exception as err:
            print(err)

if __name__ == '__main__':
    #consumer = TradeConsumer(host = 'kafka', port = '9092', group_id='BTC')
    consumer = TradeConsumer(host = 'localhost', port = '29092', group_id='BTC')
    consumer.consume(['BTC'], consumer.writeDB)