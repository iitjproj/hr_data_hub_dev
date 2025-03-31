# Databricks notebook source
# Set the AWS storage paths for source and destination
source_path = "s3://hr-data-hub/hr_data_hub_raw_src_files/subscriber.csv"

# Define the schema for the data

schema = "sub_id string  , first_name string  , last_name string  , street string  , birth_date string  , gender string  , phone string  , country string  , city string  , zip_code string  , subgrp_id string  , elig_ind string  , eff_date string  , term_date string"

# Read only the specified columns from the source file
df = spark.read \
  .option("header", "true") \
  .option("inferSchema", "false") \
  .option("delimiter", ",") \
  .option("nullValue", "") \
  .option("quote", "\"") \
  .option("escape", "\"") \
  .schema(schema) \
  .csv(source_path) \
  .select("sub_id","first_name","last_name","street","birth_date","gender","phone","country","city","zip_code","subgrp_id","elig_ind","eff_date","term_date")


df.show()

# Write the DataFrame into a catalog table with overwrite mode and enable schema evolution
df.write \
  .mode("overwrite") \
  .option("mergeSchema", "true") \
  .saveAsTable("hr_data_hub_dev_release.bronze.subscriber")