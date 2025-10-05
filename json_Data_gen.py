# Databricks notebook source

stage_table_cus = "/Volumes/cus_book_external/default/test-vol/customer/"
stage_table_book = "/Volumes/cus_book_external/default/test-vol/booking/"
print(stage_table_cus)
print(stage_table_book)

# COMMAND ----------

import random
import string
import datetime
import json


try:
    arrival_date = dbutils.widgets.get("arrival_date")
except Exception:
    arrival_date = datetime.datetime.now().strftime("%Y%m%d")

# Helper: random name generator
def random_name():
    first = ''.join(random.choice(string.ascii_letters) for _ in range(random.randint(3, 6))).capitalize()
    last = ''.join(random.choice(string.ascii_letters) for _ in range(random.randint(4, 7))).capitalize()
    return f"{first} {last}"

# Helper: random email
def random_email(name):
    return name.lower().replace(" ", ".") + "@example.com"

# Helper: random phone number
def random_phone():
    return ''.join(random.choice(string.digits) for _ in range(10))

# Helper: random date
def random_date(start_year=2023, end_year=2025):
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + datetime.timedelta(days=random_days)).isoformat()

# Generate Customers
def generate_customers(n=5):
    statuses = ["active", "inactive"]
    customers = []
    for i in range(1, n+1):
        name = random_name()
        customers.append({
            "customer_id": f"C{str(i).zfill(3)}",
            "name": name,
            "email": random_email(name),
            "phone_number": random_phone(),
            "signup_date": random_date(),
            "status": random.choice(statuses)
        })
    return customers

# Generate Bookings
def generate_bookings(customers, n=5):
    statuses = ["completed", "cancelled", "pending"]
    bookings = []
    for i in range(1, n+1):
        cust = random.choice(customers)
        booking_date = random_date(2024, 2025)
        start_time = datetime.datetime.fromisoformat(booking_date) + datetime.timedelta(hours=random.randint(8, 12))
        end_time = start_time + datetime.timedelta(hours=random.randint(2, 8))

        bookings.append({
            "booking_id": f"B{str(i).zfill(3)}",
            "customer_id": cust["customer_id"],
            "car_id": "CAR" + ''.join(random.choice(string.digits) for _ in range(3)),
            "booking_date": booking_date,
            "start_time": start_time.isoformat() + "Z",
            "end_time": end_time.isoformat() + "Z",
            "total_amount": round(random.uniform(50, 300), 2),
            "status": random.choice(statuses)
        })
    return bookings

# Generate Data
num_cus=20
customers = generate_customers(num_cus)
bookings = generate_bookings(customers, num_cus)

with open(f"{stage_table_cus}customers_{arrival_date}.json", "w") as f:
    for cus in customers:
        f.write(json.dumps(cus) + "\n")


with open(f"{stage_table_book}bookings_{arrival_date}.json", "w") as f:
    for book in bookings:
        f.write(json.dumps(book) + "\n") 
print("Data generation successfull for customers and bookings")


# COMMAND ----------

