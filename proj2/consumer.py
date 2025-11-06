"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

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


import json
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
# from employee import Employee
from producer import employee_topic_name, dlq_topic_name
from datetime import datetime
import time

# import sys
# sys.stdout.reconfigure(encoding='utf-8')
# sys.stderr.reconfigure(encoding='utf-8')


schema_name = "public"
table_employee = "employees"

class cdcConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': False, # Change to false
                     'auto.offset.reset': 'latest'}
        super().__init__(self.conf)
        self.keep_runnning = True
        self.group_id = group_id
        
        # Adding a DQL producer
        self._dlq_producer = Producer({'bootstrap.servers': f'{host}:{port}'})

    def create_tables(self):
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port = '5433',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            #your logic should go here

            ### 1) Employee table
            cur.execute(f"""
                create table if not exists {schema_name}.{table_employee} (
                    emp_id int primary key, 
                    first_name varchar(100), 
                    last_name varchar(100), 
                    dob date, 
                    city varchar(100),
                    salary int
                )
                """)
            print("Employee table created!", flush = True)
            
            
            cur.close()
        except Exception as err:
            print(f"Error occurred: {err}")
        
        return 

    def consume(self, topics, processing_func):
        self.subscribe(topics)
        while self.keep_runnning:
            #implement your logic here
            msg = self.poll(timeout=1.0)

            if msg is None:
                print("== Waiting for messages ==", flush=True)
                continue

            if msg.error():
                print(f"poll error: {msg.error()}", flush=True)
                continue

            print(f"got message: key={msg.key()} bytes={len(msg.value())}", flush=True)
            if not msg.key():
                continue
            

            retries = 0
            max_retries = 3
            backoff = 0.5

            while True:
                try:
                    processing_func(msg)

                    # commit after a successful operation
                    self.commit(msg)
                    break  # done with this message

                except Exception as err:
                    retries += 1
                    print(f"consumer process error attempt {retries}: {err}", flush=True)

                    if retries > max_retries:
                        # Build DLQ metadata
                        payload = {
                            "original_topic": msg.topic(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "timestamp": msg.timestamp()[1],
                            "group_id": self.group_id,
                            "retries": retries - 1,
                            "exception": str(err),
                            "key": msg.key().decode('utf-8') if msg.key() else None,
                            "value": msg.value().decode('utf-8') if msg.value() else None
                        }
                        try:
                            self._dlq_producer.produce(dlq_topic_name, json.dumps(payload).encode('utf-8'))
                            self._dlq_producer.flush(5)
                            print("[consume] sent to DLQ", flush=True)
                        finally:
                            # Commit the failed message
                            self.commit(msg)
                        break
                    else:
                        time.sleep(backoff)
                        backoff = min(backoff * 2, 5.0) # Cap at 5 seconds
                        

        self.close()


def update_dst(msg):
    e = Employee(**(json.loads(msg.value())))
    # print(e.action, flush = True)

    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            port = '5433', # change this port number to align with the docker compose file
            password="postgres")
        conn.autocommit = True
        cur = conn.cursor()
        #your logic goes here

        action_id = e.action_id
        emp_id = e.emp_id
        emp_FN = e.emp_FN
        emp_LN = e.emp_LN
        emp_dob = e.emp_dob
        emp_city = e.emp_city
        emp_salary = e.emp_salary
        action = e.action

        print(e.action, e.emp_FN, e.emp_FN, flush = True)

        if action in {'UPDATE', 'INSERT'}:
            cur.execute(
            f"insert into {schema_name}.{table_employee} (emp_id, first_name, last_name, dob, city, salary)"
            """
            values (%s, %s, %s, %s, %s, %s)
            on conflict (emp_id)
            do update set
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                dob = excluded.dob,
                city = excluded.city,
                salary = excluded.salary
            where (employees.first_name, employees.last_name, employees.dob, employees.city, employees.salary)
             is distinct from
               (excluded.first_name, excluded.last_name, excluded.dob, excluded.city, excluded.salary);""",
            (emp_id, emp_FN, emp_LN, datetime.strptime(emp_dob, "%Y-%m-%d").date(), emp_city, emp_salary)
            )

            print(f"Inserted/Updated one record!", flush = True)


        elif action == 'DELETE':
            cur.execute(f"""
            delete from {schema_name}.{table_employee}
            where 
            emp_id = %s and
            first_name = %s and
            last_name = %s and
            dob = %s and
            city = %s and
            salary = %s
            ;
            """,
            (emp_id, emp_FN, emp_LN, datetime.strptime(emp_dob, "%Y-%m-%d").date(), emp_city, emp_salary)
            )
            print(f"Deleted one record!", flush = True)

        cur.close()
    except Exception as err:
        print(f"Error occurred in update: {err}")
        raise

if __name__ == '__main__':
    consumer = cdcConsumer(group_id='employee-update-grp1-test0')
    consumer.create_tables()
    consumer.consume([employee_topic_name], update_dst)