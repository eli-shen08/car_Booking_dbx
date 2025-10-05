# Databricks notebook source
import datetime

try:
    arrival_date = dbutils.widgets.get("arrival_date")
except Exception:
    arrival_date = datetime.datetime.now().strftime("%Y%m%d")

bookings_path = f"/Volumes/cus_book_external/default/test-vol/booking/bookings_{arrival_date}.json"

# COMMAND ----------

df_book = spark.read.format('json').load(bookings_path)
df_book.printSchema()

# COMMAND ----------

df_book.show()

# COMMAND ----------

# df_cus.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

# Type casting into proper timestamp format

df_book = df_book.withColumn('end_time', to_timestamp(col('end_time'), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
            .withColumn('start_time', to_timestamp(col('start_time'),  "yyyy-MM-dd'T'HH:mm:ss'Z'"))
         
df_book.show()

# COMMAND ----------

# Checking invalid records and filtering valid ones
df_book_invalid = df_book.filter(
    (col("booking_id").isNull()) |
    (col("car_id").isNull()) |
    (col("customer_id").isNull()) |
    (col("booking_date").isNull()) 

)

df_book_valid = df_book.filter(
    (col("booking_date").isNotNull()) &
    (col("booking_id").isNotNull()) &
    (col("car_id").isNotNull()) &
    (col("customer_id").isNotNull())
)
invalid_count = df_book_invalid.count()
valid_count = df_book_valid.count()
print(f"Invalid records: {invalid_count}")
print(f"Valid records : {valid_count}")

# COMMAND ----------

# Ensuring that the status column contains one of the following values: ["completed", "cancelled", "pending"]

statuses = ["completed", "cancelled", "pending"]

df_book_valid = df_book_valid.filter(col("status").isin(statuses))

# COMMAND ----------

# Parse start_time and end_time into separate date and timestamp columns and also findind the total duration of booking.
df_book_valid = df_book_valid.withColumn("start_Date",to_date(col("start_time"))) \
                .withColumn("start_Timestamp",concat(hour(col("start_time")),lit(":"),minute(col("start_time")),lit(":"),second(col("start_time")))) \
                .withColumn("end_Date",to_date(col("end_time"))) \
                .withColumn("end_Timestamp",concat(hour(col("end_time")),lit(":"),minute(col("end_time")),lit(":"),second(col("end_time")))) \
                .withColumn("durationInSeconds",unix_timestamp(col("end_time")) - unix_timestamp(col("start_time"))) \
                .withColumn("durationInHours",col("durationInSeconds")/3600)

df_book_valid.select("booking_date","booking_id","start_Date","start_Timestamp","end_Date","end_Timestamp","durationInSeconds","durationInHours").show()

# COMMAND ----------


# Savinf into Delta Table
df_book_valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("`cus_book_external`.default.bookings_delta")

# COMMAND ----------

