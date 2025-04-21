from .base_strategy import BaseIngestionStrategy
from pyspark.sql.functions import col, explode, split, regexp_replace, trim
import logging
import re
import traceback
import os
import csv
import json
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row

logger = logging.getLogger(__name__)

class Type1IngestionStrategy(BaseIngestionStrategy):
    def process_csv(self, file_path):
        """Process CSV file using Type 1 strategy"""
        try:
            logger.info(f"Starting to process CSV file: {file_path}")
            
            # Validate file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found at path: {file_path}")
            
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(file_path)
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            
            # Create file paths
            address_file = os.path.join(output_dir, f"{base_name}_address.csv")
            charges_file = os.path.join(output_dir, f"{base_name}_data.csv")
            
            logger.info(f"Will create address file at: {address_file}")
            logger.info(f"Will create charges file at: {charges_file}")
            
            # Read and process the CSV file
            with open(file_path, 'r') as csvfile:
                reader = csv.reader(csvfile)
                all_rows = list(reader)
                
                # Validate file structure
                if len(all_rows) < 4:
                    raise ValueError(f"CSV file must have at least 4 rows. Found only {len(all_rows)} rows.")
                
                # Process address data (first two rows)
                address_row = all_rows[1][:5]  # Second row as data, first 5 columns
                
                # Create pandas DataFrame for address data
                address_data = [{
                    'hospital_name': address_row[0].strip() if len(address_row) > 0 else '',
                    'last_updated_on': address_row[1].strip() if len(address_row) > 1 else '',
                    'version': address_row[2].strip() if len(address_row) > 2 else '',
                    'hospital_location': address_row[3].strip() if len(address_row) > 3 else '',
                    'hospital_address': address_row[4].strip() if len(address_row) > 4 else ''
                }]
                address_df = pd.DataFrame(address_data)
                
                # Write address data to CSV for backup
                with open(address_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['hospital_name', 'last_updated_on', 'version', 'hospital_location', 'hospital_address'])
                    writer.writerow(address_row)
                
                # Get the actual header (row 3) and data rows for charges
                charges_header = all_rows[2]  # Row with column names
                data_rows = all_rows[3:]  # Actual data rows
                
                if not data_rows:
                    raise ValueError("No data rows found after header row")
                
                # Write charges data to CSV for backup
                with open(charges_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(charges_header)
                    writer.writerows(data_rows)
                
                # Read the charges file with PySpark
                hospital_schema = StructType([
                    StructField("description", StringType(), True),
                    StructField("standard_charge_gross", StringType(), True),
                    StructField("payer_name", StringType(), True),
                    StructField("plan_name", StringType(), True),
                    StructField("standard_charge_negotiated_dollar", StringType(), True),
                    StructField("standard_charge_min", StringType(), True),
                    StructField("standard_charge_max", StringType(), True)
                ])
                
                hospital_df = self.spark.read \
                    .option("header", "true") \
                    .schema(hospital_schema) \
                    .csv(charges_file)
                
                logger.info("Successfully created DataFrames")
                logger.info(f"Address DataFrame Schema: {address_df.columns.tolist()}")
                logger.info(f"Address DataFrame Count: {len(address_df)}")
                
                return address_df, hospital_df
                
        except Exception as e:
            error_msg = f"Error processing CSV file: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            raise type(e)(error_msg) from e
    
    def read_json(self, file_path):
        """Read JSON file using Type 1 strategy"""
        try:
            # Define schemas
            address_schema = StructType([
                StructField("hospital_name", StringType(), True),
                StructField("last_updated_on", StringType(), True),
                StructField("version", StringType(), True),
                StructField("hospital_location", StringType(), True),
                StructField("hospital_address", StringType(), True)
            ])
            
            hospital_schema = StructType([
                StructField("description", StringType(), True),
                StructField("standard_charge_gross", StringType(), True),
                StructField("payer_name", StringType(), True),
                StructField("plan_name", StringType(), True),
                StructField("standard_charge_negotiated_dollar", StringType(), True),
                StructField("standard_charge_min", StringType(), True),
                StructField("standard_charge_max", StringType(), True)
            ])
            
            # Read JSON file with multiline option
            df = self.spark.read \
                .option("multiline", "true") \
                .json(file_path)
            
            # Extract address data
            address_df = df.select(
                col("hospital_name"),
                col("last_updated_on"),
                col("version"),
                col("hospital_location"),
                col("hospital_address")
            ).limit(1)
            
            # Extract hospital data
            hospital_df = df.select(
                col("description"),
                col("standard_charge_gross"),
                col("payer_name"),
                col("plan_name"),
                col("standard_charge_negotiated_dollar"),
                col("standard_charge_min"),
                col("standard_charge_max")
            )
            
            return address_df, hospital_df
            
        except Exception as e:
            logger.error(f"Error reading JSON file: {str(e)}")
            raise 