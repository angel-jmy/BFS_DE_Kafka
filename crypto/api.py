"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project demo.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""


from confluent_kafka import Producer
import confluent_kafka
from confluent_kafka.serialization import StringSerializer
import os,time
import requests
import json
from datetime import datetime

class cryptoProducer(Producer):
    def __init__(self, host="localhost", port="29092",value_serializer=lambda v: json.dumps(v).encode('utf-8')):
        self.host = host
        self.port = port
        self.value_serializer = value_serializer
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)

    def produce_message(self, topic, message):
        # Ensure the message is serialized using the provided value_serializer
        serialized_message = self.value_serializer(message)
        self.produce(topic, serialized_message)
        
class coinCapProduct():
    def __init__(self,api_key,product_id):
        self.api_key = api_key
        self.url = 'https://api.coincap.io/v2/assets'+f'/{product_id}'
        self.product_id = product_id

    def getMessage(self):
        Authorization =  f"Bearer {self.api_key}"
        headers = {"Authorization":Authorization}
        response = requests.request("GET",self.url,headers = headers)
        current_time = datetime.now() #this is prob a bad practice, should be included inside response. But what do you expect from free API.
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            message = data['data']
            message['time'] = current_time.isoformat()
        else:
            print(f"Failed to fetch data: {response.status_code}")
            message = None
        return message
    
    def getAvailability(self, product_id):
        #return any kinds of data you
        pass


if __name__ == "__main__":
    api_key = "" #Use your own api key, or you can choose not to use it. 
    
    """
    If running the Python file directly on your local machine, 
    the code is configured to connect to a Kafka broker using the hostname "kafka", 
    which is only resolvable within your Docker network.

    Thus if running within docker: BTCProducer = cryptoProducer(host="kafka", port="29092")
    And if running on a local machine: BTCProducer = cryptoProducer(host="localhost",port="29092")
    """
    BTCProducer = cryptoProducer()
    #if running as a Docker container
    #BTCProducer = cryptoProducer(host="kafka", port="29092")
    BTCProduct = coinCapProduct(api_key,'bitcoin')

    encoder = StringSerializer('utf-8')

    while True:
        message = BTCProduct.getMessage()
        if message:
            print(message)
            BTCProducer.produce_message('BTC', message)
            BTCProducer.flush()
        time.sleep(3)