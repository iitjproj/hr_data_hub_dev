# Databricks notebook source
# Set the AWS storage paths for source and destination
source_path = "s3://hr-data-hub/hr_data_hub_raw_src_files/claims.json"

# Define the schema for the data

schema = "claim_id string  , patient_id string  , disease_name string  , SUB_ID string  , Claim_Or_Rejected string  , claim_type string  , claim_amount string  , claim_date string "

# Read only the specified columns from the source file
df = spark.read \
  .option("header", "true") \
  .option("inferSchema", "false") \
  .option("delimiter", ",") \
  .option("nullValue", "") \
  .option("quote", "\"") \
  .option("escape", "\"") \
  .schema(schema) \
  .json(source_path) \
  .select("claim_id","patient_id","disease_name","SUB_ID","Claim_Or_Rejected","claim_type","claim_amount","claim_date")


df.show()

# Write the DataFrame into a catalog table with overwrite mode and enable schema evolution
df.write \
  .mode("overwrite") \
  .option("mergeSchema", "true") \
  .saveAsTable("hr_data_hub_dev_release.bronze.claims")