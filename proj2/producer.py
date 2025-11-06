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

import csv
import json
import os
from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
from pyspark.sql import SparkSession
import pandas as pd
from confluent_kafka.serialization import StringSerializer
import psycopg2
import random

employee_topic_name = "bf_employee_cdc"
csv_file = "employees.csv"
schema_name = "public"
table_employee = "employees"
table_emp_cdc = "emp_cdc"
trigger_name = "employee_sync"

class cdcProducer(Producer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
        self.running = True

    def load_csv(self, path):
        df = pd.read_csv(path)
        lines = []
        for idx, row in df.iterrows():
            eid = row['Employee ID']
            efn = row['First Name']
            eln = row['Last Name']
            edob = row['Date of Birth']
            ecity = row['City']
            lines.append([eid, efn, eln, edob, ecity, 'INSERT'])

        return lines

    def load_csv_to_db(self, emps)
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            #your logic should go here
            for emp in emps:
                cur.execute(
                    f"""
                    insert into {schema_name}.{table_employee} (emp_id, first_name, last_name, dob, city, salary)
                    values (%s, %s, %s, %s, %s, %s)
                    on conflict (emp_id)
                    do update set
                    first_name = excluded.first_name
                    last_name = excluded.last_name
                    dob = excluded.dob
                    city = excluded.city
                    salary = excluded.salary
                    """
                    (emp[0], emp[1], emp[2], emp[3], emp[4], random.randint(50000, 150000))
                    )


            cur.close()
        except Exception as err:
            pass
        
        return # if you need to return sth, modify here

    def create_tables(self):
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            #your logic should go here

            ### 1) Employee table
            cur.execute(f"""
                create table if not exists {schema_name}.{table_employee} (
                    emp_id serial, 
                    first_name varchar(100), 
                    last_name varchar(100), 
                    dob date, 
                    city varchar(100),
                    salary int
                )
                """)
            
            ### 2) Actions table
            cur.execute(f"""
                create table if not exists {schema_name}.{table_emp_cdc} (
                    emp_id serial, 
                    first_name varchar(100), 
                    last_name varchar(100), 
                    dob date, 
                    city varchar(100),
                    salary int,
                    action varchar(100)
                )
                """)
            
            cur.close()
        except Exception as err:
            pass
        
        return        
    
    def fetch_cdc(self,):
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            #your logic should go here

            # Create function
            cur.execute(
               f"""
               create or replace function {schema_name}.func_{trigger_name} ()
               returns trigger as $$
               begin
                   if tg_op = 'insert' or tg_op = 'update' then

                   insert into {schema_name}.{table_emp_cdc} 
                   (emp_id, first_name, last_name, dob, city, salary, action)
                   values (new.emp_id, new.first_name, new.dob, new.city, new.salary, tg_op);

                   elsif tg_op = 'delete' then

                   insert into {schema_name}.{table_emp_cdc} 
                   (emp_id, first_name, last_name, dob, city, salary, action)
                   values (old.emp_id, old.first_name, old.dob, old.city, old.salary, tg_op);

                   end if;

                   return null;

               end;
               $$ LANGUAGE plpgsql;
               """)


            ### Create trigger from function
            cur.execute(f"""
                create trigger {schema_name}.{trigger_name}
                after insert or update or delete on {schema_name}.{table_employee}
                for each row
                execute function {schema_name}.func_{trigger_name} ()
                """)


            cur.close()
        except Exception as err:
            print(f"Error occurred: {err}")
        
        return # if you need to return sth, modify here
    

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    producer = cdcProducer()
    emps = producer.load_csv(csv_file)
    self.create_tables()
    producer.load_csv_to_db(emps)
    
    while producer.running:
        # your implementation goes here
        self.fetch_cdc()
        

    
