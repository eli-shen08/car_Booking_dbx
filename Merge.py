# Databricks notebook source
# Import required libraries
from pyspark.sql.functions import * 
from pyspark.sql.types import *

book_table = "`cus_book_external`.default.bookings_delta"
cus_table = "`cus_book_external`.default.customers_delta"
merge_target = "`cus_book_external`.default.merge_delta"

try:
    df_book = spark.read.table(book_table)
    df_cus = spark.read.table(cus_table)
except Exception as e:
    print(f"Error loading enriched datasets: {str(e)}")


# COMMAND ----------

# Renaming status col to b_status
df_book = df_book.withColumnRenamed("status", "b_status")
df_book.show()

# COMMAND ----------

# Renaming status col to c_status
df_cus = df_cus.withColumnRenamed("status", "c_status")
df_cus.show()

# COMMAND ----------

df_book_enrich = df_book.alias("b").join(df_cus.alias("a"), on="customer_id", how="inner")
df_book_enrich.columns

# COMMAND ----------

from delta.tables import *

try:
    if spark.catalog.tableExists(merge_target):
        target = DeltaTable.forName(spark, merge_target)
        (
            target.alias("t").merge(
                df_book_enrich.alias("s"),
                "t.booking_id = s.booking_id OR t.customer_id = s.customer_id") \
                    .whenMatchedUpdate(
                    set={
                        "customer_id":"s.customer_id",
                        "booking_date":"s.booking_date",
                        "booking_id":"s.booking_id",
                        "car_id":"s.car_id",
                        "end_time":"s.end_time",
                        "start_time":"s.start_time",
                        "b_status":"s.b_status",
                        "c_status":"s.c_status",
                        "total_amount":"s.total_amount",
                        "start_Date":"s.start_Date",
                        "start_Timestamp":"s.start_Timestamp",
                        "end_Date":"s.end_Date",
                        "end_Timestamp":"s.end_Timestamp",
                        "durationInSeconds":"s.durationInSeconds",
                        "durationInHours":"s.durationInHours",
                        "email":"s.email",
                        "name":"s.name",
                        "phone_number":"s.phone_number",
                        "signup_date":"s.signup_date",
                        "tenure_days":"s.tenure_days"
                    }   
                    ) 
                    .whenMatchedDelete(condition="s.status = cancelled")
                    .whenNotMatchedInsertAll()
                    .execute()
        )
    else:
        # If the table doesn't exist, just write the first version
        df_book_enrich.write.format("delta").mode("append").saveAsTable(merge_target)
except Exception as e:
    print(f"Error merging datasets: {str(e)}")


df_mer = spark.read.table(merge_target)
df_mer.show()

# COMMAND ----------

