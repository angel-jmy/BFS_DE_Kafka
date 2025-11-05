from employee import Employee
import producer as p
import consumer as c
from consumer import add_salary
from confluent_kafka.serialization import StringSerializer
import confluent_kafka as ck
import admin

def main():
    '''
    A main function that starts from the begging to end, for quick demo only
    '''

    client = admin.salaryClient()

    employee_topic_name = "bf_employee_salary"
    num_parition = 3
    if not client.topic_exists(employee_topic_name):
        client.create_topic(employee_topic_name, num_parition)

    csv_file = 'Employee_Salaries.csv'

    encoder = StringSerializer('utf-8')
    reader = p.CsvHandler()
    producer = p.salaryProducer()
    
    df = reader.read_csv(csv_file)
    lines = reader.transform(df)
    print(f"total entries: {len(lines)}")
    for line in lines:
        emp = Employee.from_csv_line(line)
        producer.produce(employee_topic_name, key=encoder(emp.emp_dept), value=encoder(emp.to_json()))
        #print(producer.producer.list_topics())
        producer.poll(1)
        #producer.flush()
    
    # consume after producing
    consumer = c.SalaryConsumer(group_id="employee_consumer_salary")
    print(client.describe_consumer_groups(["employee_consumer_salary"]))
    consumer.consume([employee_topic_name], add_salary)

if __name__ == '__main__':
    main()
    