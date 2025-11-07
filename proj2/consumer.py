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

from pathlib import Path
from confluent_kafka import DeserializingConsumer, Producer  # keep Producer for DLQ if you use it
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

sr = SchemaRegistryClient({"url": "http://localhost:8081"})
key_deser   = AvroDeserializer(sr, schema_str=open("schemas/employee_key.avsc").read())
value_deser = AvroDeserializer(sr, schema_str=open("schemas/employee_value.avsc").read())

# conf = {
#     "bootstrap.servers": "localhost:29092",
#     "group.id": "employee-update-grp1",
#     "enable.auto.commit": False,
#     "auto.offset.reset": "earliest",
#     "key.deserializer": key_deser,
#     "value.deserializer": value_deser
# }
# consumer = DeserializingConsumer(conf)

# import sys
# sys.stdout.reconfigure(encoding='utf-8')
# sys.stderr.reconfigure(encoding='utf-8')


schema_name = "public"
table_employee = "employees"

class cdcConsumer(DeserializingConsumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", \
        group_id: str = '', sr_url: str = "http://localhost:8081"):
        sr = SchemaRegistryClient({"url": sr_url})

        key_schema_str = Path("schemas/employee_key.avsc").read_text(encoding="utf-8")
        value_schema_str = Path("schemas/employee_value.avsc").read_text(encoding="utf-8")

        key_deser = AvroDeserializer(schema_registry_client=sr, schema_str=key_schema_str)
        value_deser = AvroDeserializer(schema_registry_client=sr, schema_str=value_schema_str)

        conf = {
            "bootstrap.servers": f"{host}:{port}",
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            "key.deserializer": key_deser,
            "value.deserializer": value_deser,
        }

        super().__init__(conf)
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

            ### 1) Employee table
            cur.execute(f"""
                create table if not exists {schema_name}.{table_employee} (
                    emp_id int primary key, 
                    first_name varchar(100), 
                    last_name varchar(100), 
                    dob date, 
                    city varchar(100),
                    salary float
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
            

            key_dict = msg.key()       
            val_dict = msg.value()     


            retries = 0
            max_retries = 3
            backoff = 0.5

            while True:
                try:
                    processing_func(val_dict)

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
                            "key": msg.key(),
                            "value": msg.value()
                        }
                        try:
                            self._dlq_producer.produce(dlq_topic_name, json.dumps(payload, default=str).encode('utf-8'))

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


def update_dst(d):
    # e = Employee(**(json.loads(msg.value())))
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

        emp_id     = d["emp_id"]
        emp_FN     = d["first_name"]
        emp_LN     = d["last_name"]
        emp_dob_raw= d.get("dob")          # these could be None
        emp_city   = d.get("city")
        emp_salary = d.get("salary")
        action     = d.get("action")

        dob_date = None
        if emp_dob_raw:
            dob_date = (emp_dob_raw if hasattr(emp_dob_raw, "year")
                        else datetime.strptime(emp_dob_raw, "%Y-%m-%d").date())


        print(action, emp_FN, emp_FN, flush = True)

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
            (emp_id, emp_FN, emp_LN, dob_date, emp_city, emp_salary)
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
            (emp_id, emp_FN, emp_LN, dob_date, emp_city, emp_salary)
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