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
# from pyspark.sql import SparkSession
import pandas as pd
from confluent_kafka.serialization import StringSerializer
import psycopg2
import random
import time
from datetime import datetime, timedelta

# import sys
# sys.stdout.reconfigure(encoding='utf-8')
# sys.stderr.reconfigure(encoding='utf-8')


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
        self.last_ts = datetime(1970, 1, 1)

    def load_csv(self, path):
        df = pd.read_csv(path)
        lines = []
        for idx, row in df.iterrows():
            eid = row['Employee ID']
            efn = row['First Name']
            eln = row['Last Name']
            edob = row['Date of Birth']
            ecity = row['City']
            lines.append([idx, eid, efn, eln, edob, ecity, 'INSERT'])

        print(lines, flush = True)

        return lines

    def load_csv_to_db(self, emps):
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
                print([emp[1], emp[2], emp[3], datetime.strptime(emp[4], "%Y-%m-%d").date(), emp[5]], flush = True)
                # print(datetime.strptime(emp[4], "%Y-%m-%d").date(), flush = True)
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
                    (int(emp[1]), str(emp[2]), str(emp[3]), datetime.strptime(emp[4], "%Y-%m-%d").date(), str(emp[5]), random.randint(50000, 150000))
                    )
                # cur.execute(
                #     f"insert into {schema_name}.{table_employee} (emp_id, first_name, last_name, dob, city, salary)"
                #     """
                #     values (%s, %s, %s, %s, %s, %s)
                #     on conflict (emp_id)
                #     do nothing;""",
                #     (int(emp[1]), str(emp[2]), str(emp[3]), datetime.strptime(emp[4], "%Y-%m-%d").date(), str(emp[5]), random.randint(50000, 150000))
                #     )
                print("Inserted records to table!", flush = True)


            cur.close()
        except Exception as err:
            print(f"Error occurred: {err}")
        
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
                    emp_id int primary key, 
                    first_name varchar(100), 
                    last_name varchar(100), 
                    dob date, 
                    city varchar(100),
                    salary int
                )
                """)
            print("Employee table created!", flush = True)
            
            ### 2) Actions table
            cur.execute(f"""
                create table if not exists {schema_name}.{table_emp_cdc} (
                    emp_id int, 
                    first_name varchar(100), 
                    last_name varchar(100), 
                    dob date, 
                    city varchar(100),
                    salary int,
                    action varchar(100),
                    action_id bigserial unique,
                    created_at timestamptz default now()
                )
                """)
            print("CDC table created!", flush = True)
            
            cur.close()
        except Exception as err:
            print(f"Error occurred: {err}")
        
        return 

    def setup_cdc(self):
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
            cur.execute(f"drop trigger if exists {trigger_name} on {schema_name}.{table_employee};")
            # cur.execute(f"DROP FUNCTION IF EXISTS {schema_name}.func_{trigger_name}();")

            # Create function
            cur.execute(
               f"""
               create or replace function {schema_name}.func_{trigger_name} ()
               returns trigger as $$
               begin
                   if tg_op = 'INSERT' or tg_op = 'UPDATE' then

                   insert into {schema_name}.{table_emp_cdc} 
                   (emp_id, first_name, last_name, dob, city, salary, action, created_at)
                   values (new.emp_id, new.first_name, new.last_name, new.dob, new.city, new.salary, tg_op, now());

                   elsif tg_op = 'DELETE' then

                   insert into {schema_name}.{table_emp_cdc} 
                   (emp_id, first_name, last_name, dob, city, salary, action, created_at)
                   values (old.emp_id, old.first_name, old.last_name, old.dob, old.city, old.salary, tg_op, now());

                   end if;

                   return null;

               end;
               $$ LANGUAGE plpgsql;
               """)


            ### Create trigger from function
            cur.execute(f"""
                create trigger {trigger_name}
                after insert or update or delete on {schema_name}.{table_employee}
                for each row
                execute function {schema_name}.func_{trigger_name} ()
                """)
      

            cur.close()
        except Exception as err:
            print(f"Error occurred in creating triggers: {err}")
        
        return # if you need to return sth, modify here
    
    
    def fetch_cdc(self, encoder):
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
            
            # cur.execute(f"select coalesce(max(created_at), '1970-01-01 00:00:00') FROM {schema_name}.{table_emp_cdc};")
            # (last_ts,) = cur.fetchone()

            while self.running:
                # Call produce
                
                cur.execute(f"""
                    select action_id, emp_id, first_name, last_name, dob, city, salary, action, created_at
                    from {schema_name}.{table_emp_cdc}
                    where created_at > %s;
                    """,
                    (self.last_ts,)
                    )

                rows = cur.fetchall()

                if not rows:
                    print("== Waiting for operations ==", flush = True)
                    time.sleep(1)
                    continue

                print(rows, flush = True)
                
                sent = 0
                for idx, row in enumerate(rows):
                    record = Employee.from_line(row[:-1])
                    self.produce(employee_topic_name,
                                 key = encoder(str(row[1])),
                                 value = encoder(record.to_json())
                          )
                    sent += 1

                print(f"Producer finished. Sent {sent} messages.")
                print(f"Latest timestamp: {rows[-1][-1]}.")

                self.flush(10)

                self.last_ts = rows[-1][-1] # Updating the last timestamp

                conn.commit()
                time.sleep(0.5)

            cur.close()
        except Exception as err:
            print(f"Error occurred in fech_cdc: {err}")
        
        return # if you need to return sth, modify here
    

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    producer = cdcProducer()
    emps = producer.load_csv(csv_file)
    print(emps, flush = True)

    producer.create_tables()
    


    producer.setup_cdc()
    producer.load_csv_to_db(emps)
    

    # your implementation goes here
    producer.fetch_cdc(encoder)
