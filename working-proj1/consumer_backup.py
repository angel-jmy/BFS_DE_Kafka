import json
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee


from employee import Employee
from producer import employee_topic_name

class SalaryConsumer(Consumer):
    #if connect without using a docker: host = localhost and port = 29092
    #if connect within a docker container, host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        
        #self.consumer = Consumer(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

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
                    print(f'processing {msg.value()}')
                    processing_func(msg)
        finally:
            # Close down consumer to commit final offsets.
            self.close()

def add_salary(msg):
    e = Employee(**(json.loads(msg.value())))
    try:
        conn = psycopg2.connect(
            #use localhost if not run in Docker
            host="0.0.0.0",
            database="postgres",
            user="postgres",
            port = '5432',
            password="postgres")
        conn.autocommit = True
        # create a cursor
        cur = conn.cursor()
        result = cur.execute(
        f"insert into  department_employee_salary (department,total_salary) values ('{e.emp_dept}',{int(float(e.emp_salary))}) on conflict(department) do update set total_salary = department_employee_salary.total_salary + {int(float(e.emp_salary))} "),
        '''
        if e.emp_dept == 'EMS':
            print(e.emp_salary)
        #print(result)
        '''
        #print('msg consumed')
        cur.close()
    except Exception as err:
        print(err)

if __name__ == '__main__':
    consumer = SalaryConsumer(group_id="employee_consumer_salary")
    consumer.consume([employee_topic_name], add_salary)