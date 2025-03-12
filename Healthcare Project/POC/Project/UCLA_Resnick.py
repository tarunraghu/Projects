# %%
# HEALTHCARE PROJECT

# %%
#!pip install pyspark

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("UCLA_Resnick") \
    .getOrCreate()

# Define the file path
file_path = r'C:\Users\tarun\Documents\Semester 4\Analytics Practicum\POC\UCLA_Resnick.csv'

# Load the CSV file with header
data_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# Select the specified columns
columns_to_include = ['Description', 'code|1', 'payer_name', 'plan_name', 'standard_charge|gross', 'standard_charge|max', 'standard_charge|min']
df_subset = data_df.select(*columns_to_include)

# Show the first few rows of the filtered DataFrame
df_subset.show()

# Rename the columns
df_renamed = df_subset.withColumnRenamed('description', 'billing_description') \
                      .withColumnRenamed('code|1', 'billing_code') \
                      .withColumnRenamed('payer_name', 'insurance_provider') \
                      .withColumnRenamed('plan_name', 'insurance_plan') \
                      .withColumnRenamed('standard_charge|gross', 'gross_standard_charges') \
                      .withColumnRenamed('standard_charge|max', 'max_standard_charges') \
                      .withColumnRenamed('standard_charge|min', 'min_standard_charges')

# Show the first few rows of the renamed DataFrame
df_renamed.show()

# %%
# Add a new column "hospital name" with value "UCLA_Resnick"
df_final = df_renamed.withColumn("hospital_name", lit("UCLA_Resnick"))

# Show the first few rows of the final DataFrame
df_final.show()
# %%
