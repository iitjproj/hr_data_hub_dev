# Databricks notebook source
# Set the AWS storage paths for source and destination
source_path = "s3://hr-data-hub/hr_data_hub_raw_src_files/Patient_records.csv"

# Define the schema for the data

schema = "Patient_id string,Patient_name string,patient_gender string,patient_birth_date string,patient_phone string , disease_name string , city string, hospital_id string"

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
  .select("Patient_id", "Patient_name", "patient_gender", "patient_birth_date", "patient_phone","disease_name","city","hospital_id")


df.show()

# Write the DataFrame into a catalog table with overwrite mode and enable schema evolution
df.write \
  .mode("overwrite") \
  .option("mergeSchema", "true") \
  .saveAsTable("hr_data_hub_dev_release.bronze.Patient_records")