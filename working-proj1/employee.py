import json

class Employee:
    def __init__(self,  emp_dept: str = '', emp_salary: int = 0):
        self.emp_dept = emp_dept
        self.emp_salary = emp_salary
        
    @staticmethod
    def from_csv_line(line):
        return Employee(line[0], line[1])

    def to_json(self):
        return json.dumps(self.__dict__)
