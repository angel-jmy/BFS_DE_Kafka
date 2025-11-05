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
from confluent_kafka import Consumer, KafkaError, KafkaException
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
            self.subscribe(topics)
            while self.keep_runnning:
                msg = self.poll(timeout=1.0)

                #can implement other logics for msg

                if not msg:
                    pass 
                elif msg.error():
                    pass
                else:
                    processing_func(msg)
        finally:
            # Close down consumer to commit final offsets.
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
                host="locaohost",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            #your logic goes here

            # 1) Create tables
            cur.execute(
                """
                create table if not exists department_employee (
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
                "insert into department_employee (department, department_division, position_title, hire_date, salary) "
                "values (%s, %s, %s, %s, %s)",
                (e.emp_dept, e.emp_dept_div, e.emp_pos_title, e.emp_hire_date, e.emp_salary),
            )

            # 3) upsert running total for the department
            cur.execute(
                """
                insert into public.department_employee_salary (department, total_salary)
                values (%s, %s)
                on conflict (department)
                do update set total_salary = department_totals.total_salary + excluded.total_salary;
                """,
                (e.emp_dept, e.emp_salary),
            )

            cur.close()
            conn.close()
        except Exception as err:
            print(err)

if __name__ == '__main__':
    consumer = SalaryConsumer(group_id= 'salary-consumers-grp1') #what is the group id here?
    consumer.consume([employee_topic_name],ConsumingMethods.add_salary) #what is the topic here?