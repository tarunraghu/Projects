from .base_strategy import BaseIngestionStrategy
from pyspark.sql.functions import col, explode, split, regexp_replace, trim, when
import logging
import re
import traceback
import os
import csv
import json
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import Row

logger = logging.getLogger(__name__)

class Type3IngestionStrategy(BaseIngestionStrategy):
    def process_csv(self, file_path):
        """Process CSV file using Type 3 strategy"""
        try:
            logger.info(f"Starting to process CSV file using Type 3 strategy: {file_path}")
            
            # Ensure file_path is a string
            if not isinstance(file_path, (str, bytes, os.PathLike)):
                raise TypeError(f"file_path must be a string or path-like object, not {type(file_path)}")
            
            file_path = str(file_path)  # Convert to string if it's a PathLike object
            
            # Validate file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found at path: {file_path}")
            
            # Use the existing uploads folder
            output_dir = os.path.dirname(file_path)
            
            # Handle spaces and special characters in filename
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            # Keep the original filename structure exactly as is
            safe_base_name = base_name
            
            # Create file paths in uploads folder
            address_file = os.path.join(output_dir, f"{safe_base_name}_address.csv")
            charges_file = os.path.join(output_dir, f"{safe_base_name}_data.csv")
            
            logger.info(f"Will create address file at: {address_file}")
            logger.info(f"Will create charges file at: {charges_file}")
            
            # Read and process the CSV file
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                all_rows = list(reader)
                
                # Validate file structure
                if len(all_rows) < 3:
                    raise ValueError(f"CSV file must have at least 3 rows. Found only {len(all_rows)} rows.")
                
                # Process address data (first row)
                address_row = all_rows[0][:5]  # First row as data, first 5 columns
                
                # Create pandas DataFrame for address data
                address_data = [{
                    'hospital_name': address_row[0].strip() if len(address_row) > 0 else '',
                    'last_updated_on': address_row[1].strip() if len(address_row) > 1 else '',
                    'version': address_row[2].strip() if len(address_row) > 2 else '',
                    'hospital_location': address_row[3].strip() if len(address_row) > 3 else '',
                    'hospital_address': address_row[4].strip() if len(address_row) > 4 else ''
                }]
                address_df = pd.DataFrame(address_data)
                
                # Write address data to CSV in uploads folder
                with open(address_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['hospital_name', 'last_updated_on', 'version', 'hospital_location', 'hospital_address'])
                    writer.writerow(address_row)
                
                # Verify address file was created
                if not os.path.exists(address_file):
                    raise FileNotFoundError(f"Failed to create address file at: {address_file}")
                
                # Get the actual header (row 1) and data rows for charges
                charges_header = all_rows[1]  # Row with column names
                data_rows = all_rows[2:]  # Actual data rows
                
                if not data_rows:
                    raise ValueError("No data rows found after header row")
                
                # Write charges data to CSV in uploads folder
                with open(charges_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(charges_header)
                    writer.writerows(data_rows)
                
                # Verify charges file was created
                if not os.path.exists(charges_file):
                    raise FileNotFoundError(f"Failed to create charges file at: {charges_file}")
                
                # Read the charges file with PySpark
                hospital_schema = StructType([
                    StructField("description", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("code_type", StringType(), True),
                    StructField("standard_charge_gross", DoubleType(), True),
                    StructField("payer_name", StringType(), True),
                    StructField("plan_name", StringType(), True),
                    StructField("standard_charge_negotiated_dollar", DoubleType(), True),
                    StructField("standard_charge_min", DoubleType(), True),
                    StructField("standard_charge_max", DoubleType(), True)
                ])
                
                # Read the CSV file using PySpark
                if os.path.exists(charges_file):
                    logger.info(f"Reading charges file from: {charges_file}")
                    hospital_df = self.spark.read \
                        .option("header", "true") \
                        .option("quote", '"') \
                        .option("escape", '"') \
                        .schema(hospital_schema) \
                        .csv(charges_file)
                else:
                    raise FileNotFoundError(f"Charges file not found at: {charges_file}")
                
                logger.info("Successfully created DataFrames")
                logger.info(f"Address DataFrame Schema: {address_df.columns.tolist()}")
                logger.info(f"Address DataFrame Count: {len(address_df)}")
                logger.info(f"Files saved in: {output_dir}")
                
                return address_df, hospital_df
                
        except Exception as e:
            error_msg = f"Error processing CSV file: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            raise type(e)(error_msg) from e
    
    def read_json(self, file_path):
        """Read JSON file and split into address and charges data"""
        try:
            # Validate file_path is a string
            if not isinstance(file_path, (str, bytes, os.PathLike)):
                raise TypeError(f"file_path must be a string or path-like object, not {type(file_path)}")

            file_path = str(file_path)  # Convert to string if it's a PathLike object
            logger.info(f"Starting to process JSON file: {file_path}")
            
            # Validate file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"JSON file not found at path: {file_path}")
            
            # Use the existing uploads folder
            output_dir = os.path.dirname(file_path)
            
            # Handle spaces and special characters in filename
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            # Keep the original filename structure exactly as is
            safe_base_name = base_name
            
            # Create file paths in uploads folder
            address_file = os.path.join(output_dir, f"{safe_base_name}_address.csv")
            charges_file = os.path.join(output_dir, f"{safe_base_name}_data.csv")
            
            logger.info(f"Will create address file at: {address_file}")
            logger.info(f"Will create charges file at: {charges_file}")
            
            # Read JSON file and ensure it's parsed
            with open(file_path, 'r', encoding='utf-8') as jsonfile:
                try:
                    data = json.load(jsonfile)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON: {str(e)}")
                    raise ValueError(f"Invalid JSON format: {str(e)}")
                
                if not isinstance(data, dict):
                    try:
                        if isinstance(data, str):
                            data = json.loads(data)
                        else:
                            raise ValueError(f"Expected JSON object, got {type(data)}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON string: {str(e)}")
                        raise ValueError(f"Invalid JSON string format: {str(e)}")
                
            logger.info("Successfully parsed JSON data")
            
            # Extract address data with safe navigation
            address_data = {
                'hospital_name': str(data.get('hospital_name', '')),
                'last_updated_on': str(data.get('last_updated_on', '')),
                'version': str(data.get('version', '')),
                'hospital_location': '',  # Default empty string
                'hospital_address': str(data.get('hospital_address', ''))
            }
            
            # Safely extract hospital location
            hospital_location = data.get('hospital_location', None)
            if isinstance(hospital_location, list) and len(hospital_location) > 0:
                if isinstance(hospital_location[0], dict):
                    address_data['hospital_location'] = str(hospital_location[0].get('address', ''))
            elif isinstance(hospital_location, dict):
                address_data['hospital_location'] = str(hospital_location.get('address', ''))
            elif isinstance(hospital_location, str):
                address_data['hospital_location'] = str(hospital_location)
            
            # Create address DataFrame and save to CSV
            address_df = pd.DataFrame([address_data])
            address_df.to_csv(address_file, index=False)
            logger.info(f"Address data saved to: {address_file}")
            
            # Process charges data
            standard_charge_info = data.get('standard_charge_information', [])
            if not standard_charge_info:
                logger.warning("No standard charge information found in the JSON file")
                standard_charge_info = []
            
            # Transform the data to flatten nested structures
            flattened_charges = []
            for charge in standard_charge_info:
                code_info = charge.get('code_information', [{}])[0] if charge.get('code_information') else {}
                standard_charges = charge.get('standard_charges', [])
                
                flat_charge = {
                    'description': str(charge.get('description', '')),
                    'code': str(code_info.get('code', '')),
                    'code_type': str(code_info.get('type', '')),
                    'payer_name': str(charge.get('payer_name', '')),
                    'plan_name': str(charge.get('plan_name', '')),
                    'standard_charge_gross': None,
                    'standard_charge_negotiated_dollar': None,
                    'standard_charge_min': None,
                    'standard_charge_max': None
                }
                
                # Process standard charges
                for std_charge in standard_charges:
                    charge_type = std_charge.get('type', '')
                    amount = std_charge.get('amount')
                    try:
                        amount = float(amount) if amount is not None else None
                    except (ValueError, TypeError):
                        amount = None
                        
                    if charge_type == 'gross_charge':
                        flat_charge['standard_charge_gross'] = amount
                    elif charge_type == 'discounted_cash':
                        flat_charge['standard_charge_negotiated_dollar'] = amount
                    elif charge_type == 'minimum':
                        flat_charge['standard_charge_min'] = amount
                    elif charge_type == 'maximum':
                        flat_charge['standard_charge_max'] = amount
                
                flattened_charges.append(flat_charge)
            
            if not flattened_charges:
                logger.warning("No charge data found after processing")
                # Create an empty DataFrame with the correct schema
                hospital_df = self.spark.createDataFrame([], schema=StructType([
                    StructField("description", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("code_type", StringType(), True),
                    StructField("payer_name", StringType(), True),
                    StructField("plan_name", StringType(), True),
                    StructField("standard_charge_gross", DoubleType(), True),
                    StructField("standard_charge_negotiated_dollar", DoubleType(), True),
                    StructField("standard_charge_min", DoubleType(), True),
                    StructField("standard_charge_max", DoubleType(), True)
                ]))
            else:
                # Save charges data to CSV
                charges_df = pd.DataFrame(flattened_charges)
                charges_df.to_csv(charges_file, index=False)
                logger.info(f"Charges data saved to: {charges_file}")
                
                # Verify the file was created
                if not os.path.exists(charges_file):
                    raise FileNotFoundError(f"Failed to create charges file at: {charges_file}")
                
                # Define schema for charges data
                schema = StructType([
                    StructField("description", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("code_type", StringType(), True),
                    StructField("payer_name", StringType(), True),
                    StructField("plan_name", StringType(), True),
                    StructField("standard_charge_gross", DoubleType(), True),
                    StructField("standard_charge_negotiated_dollar", DoubleType(), True),
                    StructField("standard_charge_min", DoubleType(), True),
                    StructField("standard_charge_max", DoubleType(), True)
                ])
                
                # Read the CSV file using PySpark
                if os.path.exists(charges_file):
                    logger.info(f"Reading charges file from: {charges_file}")
                    hospital_df = self.spark.read \
                        .option("header", "true") \
                        .option("quote", '"') \
                        .option("escape", '"') \
                        .schema(schema) \
                        .csv(charges_file)
                else:
                    raise FileNotFoundError(f"Charges file not found at: {charges_file}")
            
            logger.info("Successfully created DataFrames from JSON")
            logger.info(f"Address DataFrame Schema: {address_df.columns.tolist()}")
            logger.info(f"Address DataFrame Count: {len(address_df)}")
            logger.info(f"Files saved in: {output_dir}")
            
            return address_df, hospital_df
            
        except Exception as e:
            error_msg = f"Error processing JSON file: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Full stack trace:\n{traceback.format_exc()}")
            raise type(e)(error_msg) from e 