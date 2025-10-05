# Databricks notebook source
import datetime

try:
    arrival_date = dbutils.widgets.get("arrival_date")
except Exception:
    arrival_date = datetime.datetime.now().strftime("%Y%m%d")

customer_path = f"/Volumes/cus_book_external/default/test-vol/customer/customers_{arrival_date}.json"

# COMMAND ----------

df_cus = spark.read.format('json').load(customer_path)
df_cus.printSchema()

# COMMAND ----------

df_cus.show()

# COMMAND ----------

from pyspark.sql.functions import *

df_cus = df_cus.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
                             
df_cus.show()

# COMMAND ----------

# Checking invalid records and filtering valid ones
df_cus_invalid = df_cus.filter(
    (col("customer_id").isNull()) |
    (col("name").isNull()) |
    (col("email").isNull())

)

df_cus_valid = df_cus.filter(
    (col("customer_id").isNotNull()) &
    (col("name").isNotNull()) &
    (col("email").isNotNull())
)
invalid_count = df_cus_invalid.count()
valid_count = df_cus_valid.count()
print(f"Invalid records: {invalid_count}")
print(f"Valid records : {valid_count}")

# COMMAND ----------

# Validating the email columns
df_cus_valid = df_cus.filter(
    col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")
)

valid_email_count = df_cus_valid.count()
print(f"Total number of records with valid email are {valid_email_count}")

# COMMAND ----------

# Ensure status is one of the predefined statuses (e.g., active,inactive).

statuses = ["active", "inactive"]

df_cus_valid = df_cus_valid.filter(col("status").isin(statuses))

# COMMAND ----------

#  Calculate customer tenure from signup_date.
#  Might see negative tenure dates since signup_date is generated at random with in the whole year 2025 which also contains future dates. 
df_cus_valid = df_cus_valid.withColumn("tenure_days", datediff(current_date(), col("signup_date")))
df_cus_valid.show()

# COMMAND ----------

# Saving into Delta Table
df_cus_valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("`cus_book_external`.default.customers_delta")