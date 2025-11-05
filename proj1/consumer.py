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
from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_BEGINNING
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from producer import employee_topic_name # you do not want to hard copy it

class SalaryConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
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
        #implement your message processing logic here. Not necessary to follow the template. 
        try:
            # seek to beginning on assignment (belt-and-suspenders)
            def on_assign(c, partitions):
                
                for p in partitions:
                    print(f"Assigned: topic={p.topic}, partition={p.partition}, "
                          f"offset={p.offset}, error={p.error}", flush=True)
                    p.offset = OFFSET_BEGINNING
                c.assign(partitions)

            print("group.id =", self.group_id, flush=True)
            print("Subscribing to:", topics, flush=True)
            self.subscribe(topics, on_assign=on_assign)

            while self.keep_runnning:
                msg = self.poll(timeout=1.0)

                if msg is None:
                    print("poll: no message", flush=True)
                    continue

                if msg.error():
                    print(f"poll error: {msg.error()}", flush=True)
                    continue

                print(f"got message: key={msg.key()} bytes={len(msg.value())}", flush=True)
                if not msg.key():
                    continue
                    
                # print(f'processing {msg.value()}', flush = True)
                processing_func(msg)

        finally:
            self.close()

#or can put all functions in a separte file and import as a module
class ConsumingMethods:
    @staticmethod
    def add_salary(msg):


        e = Employee(**(json.loads(msg.value())))
        try:
            conn = psycopg2.connect(
                #use localhost if not run in Docker
                # host="0.0.0.0",
                host="localhost",
                # host = "host.docker.internal",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print(f"Connected to Postgres. Server version: {version[0]}")
            #your logic goes here

            # 1) Create tables
            cur.execute(
                """
                create table if not exists public.department_employee (
                  department varchar(200),
                  department_division varchar(200),
                  position_title     varchar(200),
                  hire_date          date,
                  salary             decimal
                );
                """
            )
            cur.execute(
                """
                create table if not exists public.department_employee_salary (
                  department   varchar(200) not null,
                  total_salary int4 NULL,
                  constraint department_employee_salary_pk primary key (department)
                );
                """
            )

            # Insert the detail row
            cur.execute(
                "insert into public.department_employee (department, department_division, position_title, hire_date, salary) "
                "values (%s, %s, %s, %s, %s)",
                (e.emp_dept, e.emp_dept_div, e.emp_pos_title, e.emp_hire_date, e.emp_salary),
            )

            # 3) upsert running total for the department
            cur.execute(
                """
                INSERT INTO public.department_employee_salary (department, total_salary)
                VALUES (%s, %s)
                ON CONFLICT (department)
                DO UPDATE SET total_salary = department_employee_salary.total_salary + EXCLUDED.total_salary;
                """,
                (e.emp_dept, e.emp_salary),
            )

            cur.close()
            conn.close()
        except Exception as err:
            print(err)

if __name__ == '__main__':
    consumer = SalaryConsumer(group_id= 'salary-consumers-0') #what is the group id here?
    consumer.consume([employee_topic_name],ConsumingMethods.add_salary) #what is the topic here?