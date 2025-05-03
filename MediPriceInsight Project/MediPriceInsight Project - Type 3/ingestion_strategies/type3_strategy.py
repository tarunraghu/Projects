from .base_strategy import BaseIngestionStrategy
from pyspark.sql.functions import col, explode, split, regexp_replace, trim, when
import logging
import re
import traceback
import os
import csv
import json
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
from pyspark.sql import Row
from datetime import datetime

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
                
                # Process hospital_location to handle array format
                hospital_location = address_row[3].strip() if len(address_row) > 3 else ''
                
                # For CSV processing, use the hospital name as location if location is empty or not provided
                if not hospital_location or hospital_location.strip('[]"\' ') == '':
                    hospital_location = address_row[0].strip() if len(address_row) > 0 else ''
                    logger.info(f"Using hospital name as location: {hospital_location}")
                else:
                    try:
                        # Try to parse as JSON if it's in array format
                        location_data = json.loads(hospital_location)
                        if isinstance(location_data, list) and len(location_data) > 0:
                            hospital_location = str(location_data[0])
                    except (json.JSONDecodeError, TypeError):
                        # If JSON parsing fails, try to clean up the string
                        hospital_location = hospital_location.strip('[]"\' ')

                logger.info(f"Raw hospital_location from CSV: {address_row[3]}")
                logger.info(f"Processed hospital_location: {hospital_location}")

                address_data = [{
                    'hospital_name': address_row[0].strip() if len(address_row) > 0 else '',
                    'last_updated_on': address_row[1].strip() if len(address_row) > 1 else '',
                    'version': address_row[2].strip() if len(address_row) > 2 else '',
                    'hospital_location': hospital_location,
                    'hospital_address': address_row[4].strip() if len(address_row) > 4 else ''
                }]
                address_df = pd.DataFrame(address_data)
                
                # Write address data to CSV in uploads folder
                with open(address_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['hospital_name', 'last_updated_on', 'version', 'hospital_location', 'hospital_address'])
                    row_to_write = [
                        address_data[0]['hospital_name'],
                        address_data[0]['last_updated_on'],
                        address_data[0]['version'],
                        address_data[0]['hospital_location'],
                        address_data[0]['hospital_address']
                    ]
                    logger.info(f"Writing row to CSV: {row_to_write}")  # Add logging
                    writer.writerow(row_to_write)
                
                logger.info(f"Verifying written data - reading back from {address_file}")
                with open(address_file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader)  # Skip header
                    first_row = next(reader)
                    logger.info(f"Read back from CSV - hospital_location: {first_row[3]}")
                
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
                
                # After writing the header and data rows to charges_file, add the hospital_name and default values
                # Read the CSV back with pandas to add the additional columns
                charges_df = pd.read_csv(charges_file)
                
                # Add the new columns with default values
                charges_df['hospital_name'] = address_data[0]['hospital_name']
                charges_df['is_active'] = True
                current_time = datetime.now()
                charges_df['created_at'] = current_time
                charges_df['updated_at'] = current_time
                
                # Write back to CSV
                charges_df.to_csv(charges_file, index=False)
                
                # Verify charges file was created
                if not os.path.exists(charges_file):
                    raise FileNotFoundError(f"Failed to create charges file at: {charges_file}")
                
                # Read the charges file with PySpark
                hospital_schema = StructType([
                    StructField("hospital_name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("code_type", StringType(), True),
                    StructField("payer_name", StringType(), True),
                    StructField("plan_name", StringType(), True),
                    StructField("standard_charge_gross", DoubleType(), True),
                    StructField("standard_charge_discounted_cash", DoubleType(), True),
                    StructField("standard_charge_negotiated_dollar", DoubleType(), True),
                    StructField("standard_charge_min", DoubleType(), True),
                    StructField("standard_charge_max", DoubleType(), True),
                    StructField("estimated_amount", DoubleType(), True),
                    StructField("is_active", BooleanType(), True),
                    StructField("created_at", TimestampType(), True),
                    StructField("updated_at", TimestampType(), True)
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
            try:
                with open(file_path, 'r', encoding='utf-8') as jsonfile:
                    data = json.load(jsonfile)
            except UnicodeDecodeError as e_utf8:
                logger.warning(f"UTF-8 decode failed: {e_utf8}. Trying cp1252 encoding.")
                with open(file_path, 'r', encoding='cp1252') as jsonfile:
                    data = json.load(jsonfile)
            
            logger.info("Successfully parsed JSON data")
            
            # Extract address data with safe navigation
            hospital_name = str(data.get('hospital_name', ''))
            address_data = {
                'hospital_name': hospital_name,
                'last_updated_on': str(data.get('last_updated_on', '')),
                'version': str(data.get('version', '')),
                'hospital_location': hospital_name,  # Use hospital name as location
                'hospital_address': str(data.get('hospital_address', ''))
            }
            
            # Safely extract hospital location
            hospital_location = data.get('hospital_location', None)
            if isinstance(hospital_location, list) and len(hospital_location) > 0:
                if isinstance(hospital_location[0], str):
                    address_data['hospital_location'] = hospital_location[0]
                elif isinstance(hospital_location[0], dict):
                    # Try different possible keys that might contain the location
                    loc = hospital_location[0].get('location', '')
                    if not loc:
                        loc = hospital_location[0].get('name', '')
                    if not loc:
                        loc = hospital_location[0].get('value', '')
                    if loc:
                        address_data['hospital_location'] = str(loc)
            elif isinstance(hospital_location, dict):
                loc = hospital_location.get('location', '')
                if not loc:
                    loc = hospital_location.get('name', '')
                if not loc:
                    loc = hospital_location.get('value', '')
                if loc:
                    address_data['hospital_location'] = str(loc)
            
            logger.info(f"Processed hospital location from JSON: {address_data['hospital_location']}")
            
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
                # Get all code information entries
                code_information = charge.get('code_information', [])
                standard_charges = charge.get('standard_charges', [])
                description = str(charge.get('description', ''))
                
                # If no code information, create one record with empty code info
                if not code_information:
                    code_information = [{}]
                
                # Create a record for each code
                for code_info in code_information:
                    # Only process if code type is CPT
                    code_type = str(code_info.get('type', '')).upper()  # Convert to uppercase for case-insensitive comparison
                    if code_type != 'CPT':
                        continue  # Skip non-CPT codes
                    
                    # Base charge info with specific code
                    base_charge = {
                        'hospital_name': hospital_name,  # Add hospital_name from the address data
                        'description': description,
                        'code': str(code_info.get('code', '')),
                        'code_type': code_type,
                        'payer_name': '',  # Default empty
                        'plan_name': '',   # Default empty
                        'standard_charge_gross': None,
                        'standard_charge_discounted_cash': None,
                        'standard_charge_negotiated_dollar': None,
                        'standard_charge_min': None,
                        'standard_charge_max': None,
                        'estimated_amount': None,
                        'is_active': True,  # Default to True as per schema
                        'created_at': datetime.now(),  # Current timestamp
                        'updated_at': datetime.now()   # Current timestamp
                    }

                    # Process each standard charge entry
                    for std_charge in standard_charges:
                        # If payers_information exists and is a list, create a row for each payer
                        payers_info = std_charge.get('payers_information')
                        if payers_info and isinstance(payers_info, list) and len(payers_info) > 0:
                            for payer in payers_info:
                                flat_charge = base_charge.copy()
                                if isinstance(payer, dict):
                                    flat_charge['payer_name'] = str(payer.get('payer_name', ''))
                                    flat_charge['plan_name'] = str(payer.get('plan_name', ''))
                                    # Map standard_charge_dollar to standard_charge_negotiated_dollar
                                    if 'standard_charge_dollar' in payer:
                                        try:
                                            flat_charge['standard_charge_negotiated_dollar'] = float(payer['standard_charge_dollar'])
                                        except (ValueError, TypeError):
                                            logger.warning(f"Invalid standard_charge_dollar value: {payer['standard_charge_dollar']}")
                                            flat_charge['standard_charge_negotiated_dollar'] = None
                                    # Map estimated_amount from payer to output row
                                    if 'estimated_amount' in payer:
                                        try:
                                            flat_charge['estimated_amount'] = float(payer['estimated_amount'])
                                        except (ValueError, TypeError):
                                            logger.warning(f"Invalid estimated_amount value: {payer['estimated_amount']}")
                                            flat_charge['estimated_amount'] = None
                                charge_type = std_charge.get('type', '')
                                amount = std_charge.get('amount')
                                gross_charge = std_charge.get('gross_charge')
                                if gross_charge is not None:
                                    try:
                                        flat_charge['standard_charge_gross'] = float(gross_charge)
                                    except (ValueError, TypeError):
                                        logger.warning(f"Invalid gross_charge value: {gross_charge}")
                                        flat_charge['standard_charge_gross'] = None
                                discounted_cash = std_charge.get('discounted_cash')
                                if discounted_cash is not None:
                                    try:
                                        flat_charge['standard_charge_discounted_cash'] = float(discounted_cash)
                                    except (ValueError, TypeError):
                                        logger.warning(f"Invalid discounted_cash value: {discounted_cash}")
                                        flat_charge['standard_charge_discounted_cash'] = None
                                minimum = std_charge.get('minimum')
                                if minimum is not None:
                                    try:
                                        flat_charge['standard_charge_min'] = float(minimum)
                                    except (ValueError, TypeError):
                                        logger.warning(f"Invalid minimum value: {minimum}")
                                        flat_charge['standard_charge_min'] = None
                                maximum = std_charge.get('maximum')
                                if maximum is not None:
                                    try:
                                        flat_charge['standard_charge_max'] = float(maximum)
                                    except (ValueError, TypeError):
                                        logger.warning(f"Invalid maximum value: {maximum}")
                                        flat_charge['standard_charge_max'] = None
                                try:
                                    amount = float(amount) if amount is not None else None
                                except (ValueError, TypeError):
                                    amount = None
                                if charge_type == 'gross_charge':
                                    flat_charge['standard_charge_gross'] = amount
                                elif charge_type == 'discounted_cash':
                                    flat_charge['standard_charge_discounted_cash'] = amount
                                elif charge_type == 'minimum':
                                    flat_charge['standard_charge_min'] = amount
                                elif charge_type == 'maximum':
                                    flat_charge['standard_charge_max'] = amount
                                logger.info(f"Processed charge - Description: {description}, Gross: {flat_charge['standard_charge_gross']}, Min: {flat_charge['standard_charge_min']}, Max: {flat_charge['standard_charge_max']}, Payer: {flat_charge['payer_name']}, Plan: {flat_charge['plan_name']}")
                                flattened_charges.append(flat_charge)
                        else:
                            # No payers_information or not a list, fallback to previous logic
                            flat_charge = base_charge.copy()
                            if payers_info and isinstance(payers_info, dict):
                                flat_charge['payer_name'] = str(payers_info.get('payer_name', ''))
                                flat_charge['plan_name'] = str(payers_info.get('plan_name', ''))
                            charge_type = std_charge.get('type', '')
                            amount = std_charge.get('amount')
                            gross_charge = std_charge.get('gross_charge')
                            if gross_charge is not None:
                                try:
                                    flat_charge['standard_charge_gross'] = float(gross_charge)
                                except (ValueError, TypeError):
                                    logger.warning(f"Invalid gross_charge value: {gross_charge}")
                                    flat_charge['standard_charge_gross'] = None
                            discounted_cash = std_charge.get('discounted_cash')
                            if discounted_cash is not None:
                                try:
                                    flat_charge['standard_charge_discounted_cash'] = float(discounted_cash)
                                except (ValueError, TypeError):
                                    logger.warning(f"Invalid discounted_cash value: {discounted_cash}")
                                    flat_charge['standard_charge_discounted_cash'] = None
                            minimum = std_charge.get('minimum')
                            if minimum is not None:
                                try:
                                    flat_charge['standard_charge_min'] = float(minimum)
                                except (ValueError, TypeError):
                                    logger.warning(f"Invalid minimum value: {minimum}")
                                    flat_charge['standard_charge_min'] = None
                            maximum = std_charge.get('maximum')
                            if maximum is not None:
                                try:
                                    flat_charge['standard_charge_max'] = float(maximum)
                                except (ValueError, TypeError):
                                    logger.warning(f"Invalid maximum value: {maximum}")
                                    flat_charge['standard_charge_max'] = None
                            try:
                                amount = float(amount) if amount is not None else None
                            except (ValueError, TypeError):
                                amount = None
                            if charge_type == 'gross_charge':
                                flat_charge['standard_charge_gross'] = amount
                            elif charge_type == 'discounted_cash':
                                flat_charge['standard_charge_discounted_cash'] = amount
                            elif charge_type == 'minimum':
                                flat_charge['standard_charge_min'] = amount
                            elif charge_type == 'maximum':
                                flat_charge['standard_charge_max'] = amount
                            logger.info(f"Processed charge - Description: {description}, Gross: {flat_charge['standard_charge_gross']}, Min: {flat_charge['standard_charge_min']}, Max: {flat_charge['standard_charge_max']}, Payer: {flat_charge['payer_name']}, Plan: {flat_charge['plan_name']}")
                            flattened_charges.append(flat_charge)
            
            if not flattened_charges:
                logger.warning("No charge data found after processing")
                # Create an empty DataFrame with the correct schema
                hospital_df = self.spark.createDataFrame([], schema=StructType([
                    StructField("hospital_name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("code_type", StringType(), True),
                    StructField("payer_name", StringType(), True),
                    StructField("plan_name", StringType(), True),
                    StructField("standard_charge_gross", DoubleType(), True),
                    StructField("standard_charge_discounted_cash", DoubleType(), True),
                    StructField("standard_charge_negotiated_dollar", DoubleType(), True),
                    StructField("standard_charge_min", DoubleType(), True),
                    StructField("standard_charge_max", DoubleType(), True),
                    StructField("estimated_amount", DoubleType(), True),
                    StructField("is_active", BooleanType(), True),
                    StructField("created_at", TimestampType(), True),
                    StructField("updated_at", TimestampType(), True)
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
                    StructField("hospital_name", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("code_type", StringType(), True),
                    StructField("payer_name", StringType(), True),
                    StructField("plan_name", StringType(), True),
                    StructField("standard_charge_gross", DoubleType(), True),
                    StructField("standard_charge_discounted_cash", DoubleType(), True),
                    StructField("standard_charge_negotiated_dollar", DoubleType(), True),
                    StructField("standard_charge_min", DoubleType(), True),
                    StructField("standard_charge_max", DoubleType(), True),
                    StructField("estimated_amount", DoubleType(), True),
                    StructField("is_active", BooleanType(), True),
                    StructField("created_at", TimestampType(), True),
                    StructField("updated_at", TimestampType(), True)
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