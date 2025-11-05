import json
from datetime import datetime

class Employee:
    def __init__(self,  emp_dept: str = '', emp_dept_div: str = '', \
        emp_pos_title: str = '', emp_hire_date: datetime = None, emp_salary: int = 0):
        self.emp_dept = emp_dept
        self.emp_dept_div = emp_dept_div
        self.emp_pos_title = emp_pos_title
        self.emp_hire_date = emp_hire_date
        self.emp_salary = emp_salary
        
    @staticmethod
    def from_csv_line(line):
        return Employee(line[0], line[1], line[3], line[5], line[7])

    def to_json(self):
        return json.dumps(self.__dict__)
        # d = self.__dict__.copy()
        # if isinstance(d["emp_hire_date"], datetime):
        #     d["emp_hire_date"] = d["emp_hire_date"].date().isoformat()
        # return json.dumps(d)
