import os
import csv
import json
import sqlite3
import random
import statistics
import logging
from pathlib import Path
from datetime import datetime
import shutil
import pandas as pd
import numpy as np
import asyncio
from functools import reduce, lru_cache
from typing import List, Optional, Generator

# ------------------- Logging -------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------- Setup Paths -------------------
BASE_DIR = Path.cwd() / "async_employee_data"
BASE_DIR.mkdir(exist_ok=True)
CSV_FILE = BASE_DIR / "employees.csv"
DB_FILE = BASE_DIR / "employees.sqlite"
JSON_FILE = BASE_DIR / "employees.json"

# ------------------- Constants -------------------
country_stats = {
    "USA": {"avg_salary": 65000, "std_salary": 15000, "avg_hours": 34, "employment_rate": 0.74},
    "Germany": {"avg_salary": 55000, "std_salary": 12000, "avg_hours": 34, "employment_rate": 0.72},
    "India": {"avg_salary": 12000, "std_salary": 5000, "avg_hours": 48, "employment_rate": 0.56},
    "Brazil": {"avg_salary": 18000, "std_salary": 6000, "avg_hours": 40, "employment_rate": 0.60},
    "Japan": {"avg_salary": 60000, "std_salary": 13000, "avg_hours": 42, "employment_rate": 0.73}
}

departments = ["HR", "Finance", "IT", "Marketing", "Operations"]
names = [

"Alice","Bob","Charlie","Diana","Ethan","Fiona","George","Hannah","Ivan","Julia",
         "Karl","Laura","Mike","Nina","Oscar","Paula","Quinn","Ravi","Sofia","Tom","Uma","Vik","Wendy","Xander","Yara","Zane"
         ]

# ------------------- Decorators -------------------
def log_action(func):
    async def wrapper(*args, **kwargs):
        logging.info(f"Calling {func.__name__}")
        result = await func(*args, **kwargs)
        logging.info(f"Completed {func.__name__}")
        return result
    return wrapper

# ------------------- Employee Classes -------------------
class Employee:
    def __init__(self, emp_id: str, name: str, age: int, department: str, salary: float, hours: float, country: str):
        self.__emp_id = emp_id
        self.name = name
        self.age = age
        self.department = department
        self.salary = salary
        self.hours = hours
        self.country = country
        self.join_date = datetime.now().date()

    @property
    def emp_id(self):
        return self.__emp_id

    async def apply_raise(self, percent: float):
        self.salary *= (1 + percent / 100)

    def __repr__(self):
        return f"Employee({self.__emp_id}, {self.name}, {self.country}, ${self.salary:.2f})"

class Manager(Employee):
    def __init__(self, emp_id, name, age, department, salary, hours, country, team: Optional[List[Employee]]=None):
        super().__init__(emp_id, name, age, department, salary, hours, country)
        self.team = team if team else []

    async def apply_raise(self, percent: float):
        await super().apply_raise(percent + 5)

# ------------------- Company Class -------------------
class Company:
    def __init__(self, name: str):
        self.name = name
        self.employees: List[Employee] = []

    def add_employee(self, emp: Employee):
        self.employees.append(emp)

    def __iter__(self) -> Generator[Employee, None, None]:
        for e in self.employees:
            yield e

    @log_action
    async def save_to_csv(self):
        await asyncio.to_thread(self._save_csv)

    def _save_csv(self):
        with open(CSV_FILE,'w',newline='',encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["ID","Name","Age","Department","Salary","Hours","Country","JoinDate"])
            for e in self.employees:
                writer.writerow([e.emp_id,e.name,e.age,e.department,e.salary,e.hours,e.country,e.join_date])

    @log_action
    async def save_to_json(self):
        await asyncio.to_thread(self._save_json)

    def _save_json(self):
        data = [e.__dict__ for e in self.employees]
        for d in data:
            d.pop("_Employee__emp_id", None)
        with open(JSON_FILE,'w',encoding='utf-8') as f:
            json.dump(data,f,default=str)

    @log_action
    async def save_to_sqlite(self):
        await asyncio.to_thread(self._save_sqlite)

    def _save_sqlite(self):
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                emp_id TEXT PRIMARY KEY,
                name TEXT,
                age INTEGER,
                department TEXT,
                salary REAL,
                hours REAL,
                country TEXT,
                join_date TEXT
            )
        """)
        for e in self.employees:
            cur.execute("INSERT OR REPLACE INTO employees VALUES (?,?,?,?,?,?,?,?)",
                        (e.emp_id,e.name,e.age,e.department,e.salary,e.hours,e.country,str(e.join_date)))
        conn.commit()
        conn.close()

# ------------------- Random Employee Generator -------------------
def generate_employee(emp_id: str, country: str) -> Optional[Employee]:
    stats = country_stats[country]
    name = random.choice(names)
    age = random.randint(22,60)
    department = random.choice(departments)

    # Random salary distributions
    dist = random.choice(["gauss","normal","lognormal","exponential","beta","pareto","vonmises"])
    if dist=="gauss": salary = round(random.gauss(stats["avg_salary"],stats["std_salary"]),2)
    elif dist=="normal": salary = round(random.normalvariate(stats["avg_salary"],stats["std_salary"]),2)
    elif dist=="lognormal": salary = round(random.lognormvariate(np.log(stats["avg_salary"]),0.4),2)
    elif dist=="exponential": salary = round(random.expovariate(1/stats["avg_salary"]),2)
    elif dist=="beta": salary = round(random.betavariate(2,5)*stats["avg_salary"]*2,2)
    elif dist=="pareto": salary = round(random.paretovariate(2)*stats["avg_salary"]/2,2)
    else: salary = round(random.vonmisesvariate(0,4)*stats["avg_salary"]/10 + stats["avg_salary"],2)

    hours = round(random.gauss(stats["avg_hours"],2) + random.uniform(-1,1),1)
    if random.random() > stats["employment_rate"]:
        return None
    return Employee(emp_id,name,age,department,salary,hours,country)

# ------------------- Async Employee Creation -------------------
async def create_employees_async(company: Company, emp_count: int = 100):
    emp_id = 1
    tasks = []
    for country in country_stats.keys():
        added = 0
        while added < emp_count:
            emp = generate_employee(f"EMP{emp_id:04d}", country)
            if emp:
                company.add_employee(emp)
                added += 1
            emp_id += 1
    logging.info(f"Created {len(company.employees)} employees")

# ------------------- Async Salary Raise -------------------
async def raise_salaries_async(employees: List[Employee], percent: float):
    tasks = [e.apply_raise(percent) for e in employees]
    await asyncio.gather(*tasks)

# ------------------- Main Async Routine -------------------
async def main():
    company = Company("AsyncTech")

    # Create Employees
    await create_employees_async(company, emp_count=100)

    # Promote some to Managers
    managers = random.sample(company.employees,10)
    for m in managers:
        mgr = Manager(m.emp_id,m.name,m.age,m.department,m.salary,m.hours,m.country,
                      team=random.sample(company.employees,5))
        company.employees[company.employees.index(m)] = mgr

    # Compute Statistics
    salaries = [e.salary for e in company]
    hours = [e.hours for e in company]
    logging.info(f"Average Salary: {statistics.mean(salaries):.2f}")
    logging.info(f"Top 5 Salaries: {sorted(salaries)[-5:]}")

    # Save Data Async
    await asyncio.gather(
        company.save_to_csv(),
        company.save_to_json(),
        company.save_to_sqlite()
    )

    # Pandas Analysis
    df = pd.DataFrame([{
        "ID":e.emp_id,"Name":e.name,"Age":e.age,"Department":e.department,
        "Salary":e.salary,"Hours":e.hours,"Country":e.country,"JoinDate":e.join_date
    } for e in company])
    summary = df.groupby(['Country','Department']).agg({
        'Salary':['mean','std','min','max'],
        'Hours':['mean','std']
    }).round(2)
    logging.info(f"Country & Department Summary:\n{summary}")

    # Async Salary Raise
    await raise_salaries_async(random.sample(company.employees, 30), 5)

    # Archive
    await asyncio.to_thread(shutil.make_archive,str(BASE_DIR/"backup"),'zip',BASE_DIR)
    logging.info("Data archived successfully!")

# ------------------- Run Async -------------------
asyncio.run(main())
