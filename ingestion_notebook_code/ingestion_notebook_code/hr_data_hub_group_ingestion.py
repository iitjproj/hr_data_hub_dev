# Databricks notebook source
# Set the AWS storage paths for source and destination
source_path = "s3://hr-data-hub/hr_data_hub_raw_src_files/group.csv"

# Define the schema for the data
schema = "country string,premium_written string,zip_code string,grp_id string,grp_name string,grp_type string,city string"

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
  .select("country","premium_written","zip_code","grp_id","grp_name","grp_type","city")
df.show()
# Write the DataFrame into a catalog table with overwrite mode
df.write.mode("overwrite").saveAsTable("hr_data_hub_dev_release.bronze.group")