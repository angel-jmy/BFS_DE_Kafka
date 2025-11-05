import csv
import json
import os


from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
import pandas as pd
from confluent_kafka.serialization import StringSerializer


employee_topic_name = "bf_employee_salary"
csv_file = 'Employee_Salaries.csv'


class salaryProducer(Producer):
    #if connect without using a docker: host = localhost and port = 29092
    #if connect within a docker container, host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
     

class CsvHandler:
    def __init__(self):
        #self.handler = SparkSession.builder.appName('CsvHandler').getOrCreate()
        pass

    def read_csv(self, csv_file):
        df = pd.read_csv(csv_file)
        return df
        
    def transform(self,df):
        res = []
        depts = set(['ECC','CIT','EMS'])
        for index, row in df.iterrows():
            dept = row['Department']
            try:
                salary = int(row['Salary'])
                hire_year = int(row['Initial Hire Date'].split('-')[2])
            except:
                print(f'null found at {index}')
                continue #if null exists, skip current entry
            if dept in depts and hire_year >= 2010:
                #print(row)
                res.append([dept,salary])
                
        return res

if __name__ == '__main__':
    encoder = StringSerializer('utf-8')
    reader = CsvHandler()
    producer = salaryProducer()
    df = reader.read_csv(csv_file)
    lines = reader.transform(df)

    for line in lines:
        emp = Employee.from_csv_line(line)
        producer.produce(employee_topic_name, key=encoder(emp.emp_dept), value=encoder(emp.to_json()))
        producer.poll(1)
