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
import pandas as pd
from confluent_kafka.serialization import StringSerializer
from datetime import datetime


employee_topic_name = "bf_employee_salary"
csv_file = 'Employee_Salaries.csv'
dept_name = {"ECC", "CIT", "EMS"} # Allowed department names

#Can use the confluent_kafka.Producer class directly
class salaryProducer(Producer):
    #if connect without using a docker: host = localhost and port = 29092
    #if connect within a docker container, host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
     

class DataHandler:
    '''
    Your data handling logic goes here. 
    You can also implement the same logic elsewhere. Your call
    '''
    DEPT_COL = 'Department'
    DEPT_DIV_COL = 'Department-Division'
    POS_TITLE_COL = 'Position Title'
    HIRED_YEAR_COL = 'Initial Hire Date'
    SALARY_COL = 'Salary'

    def __init__(self, path: str):
        self.path = path

    def rows(self):
        with open(self.path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                dept = (row[self.DEPT_COL] or "").strip().upper()
                dept_div = row[self.DEPT_DIV_COL] or ""
                pos_title = row[self.POS_TITLE_COL] or ""

                if dept not in ALLOWED_DEPTS:
                    continue

                # Round salary down to integer dollars
                salary = int(math.floor(float(row[self.SALARY_COL])))

                # Keep only employees hired after 2010
                date_str = row[self.HIRED_YEAR_COL].strip()
                try:
                    hire_date = datetime.strptime(date_str, "%d-%b-%y")
                    hired_year = hire_date.year
                    if hired_year <= 2010:
                        continue
                except ValueError:
                    continue


                # emp_dept + emp_salary + to_json()
                yield Employee(emp_dept=dept, emp_dept_div = dept_div, emp_pos_title = pos_title, emp_hire_date = hire_date, emp_salary=salary)


def _delivery(err, msg):
    if err:
        print(f"Delivery failed: {err}")


if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    reader = DataHandler(csv_file)
    producer = salaryProducer()
    '''
    # implement other instances as needed
    # you can let producer process line by line, and stop after all lines are processed, or you can keep the producer running.
    # finish code with your own logic and reasoning

    for line in lines:
        emp = Employee.from_csv_line(line)
        producer.produce(employee_topic_name, key=encoder(emp.emp_dept), value=encoder(emp.to_json()))
        producer.poll(1)
    '''
    for emp in reader.rows():
        producer.produce(
            employee_topic_name,
            key=encoder(emp.emp_dept),     # partition by department
            value=encoder(emp.to_json()),
            on_delivery=_delivery,
        )
        producer.poll(1)

    producer.flush(10)
    print("Producer finished sending filtered employee records.")