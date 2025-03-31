# Databricks notebook source
# Set the AWS storage paths for source and destination
source_path = "s3://hr-data-hub/hr_data_hub_raw_src_files/hospital.csv"

# Define the schema for the data
schema = "hospital_id string,hospital_name string,city string,state string,country string"

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
  .select("hospital_id", "hospital_name", "city", "state", "country")

df.show()

# Write the DataFrame into a catalog table with overwrite mode and enable schema evolution
df.write \
  .mode("overwrite") \
  .option("mergeSchema", "true") \
  .saveAsTable("hr_data_hub_dev_release.bronze.hospital")