# Databricks notebook source
# Set the AWS storage paths for source and destination
source_path = "s3://hr-data-hub/hr_data_hub_raw_src_files/disease.csv"

# Define the schema for the data
schema = "subgrp_id string, disease_id string, disease_name string"

# Read only the specified columns from the source file
df = spark.read \
  .option("header", "false") \
  .option("inferSchema", "false") \
  .option("delimiter", ",") \
  .option("nullValue", "") \
  .option("quote", "\"") \
  .option("escape", "\"") \
  .schema(schema) \
  .csv(source_path) \
  .select("subgrp_id", "disease_id", "disease_name")

# Write the DataFrame into a catalog table with overwrite mode
df.write.mode("overwrite").saveAsTable("hr_data_hub_dev_release.bronze.disease")