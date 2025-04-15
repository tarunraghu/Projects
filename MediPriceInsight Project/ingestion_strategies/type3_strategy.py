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
                    # Base charge info with specific code
                    base_charge = {
                        'description': description,
                        'code': str(code_info.get('code', '')),
                        'code_type': str(code_info.get('type', '')),
                        'payer_name': '',  # Default empty
                        'plan_name': '',   # Default empty
                        'standard_charge_gross': None,
                        'standard_charge_negotiated_dollar': None,
                        'standard_charge_min': None,
                        'standard_charge_max': None
                    }

                    # Process each standard charge entry
                    for std_charge in standard_charges:
                        flat_charge = base_charge.copy()  # Create a copy of base charge with the code info
                        
                        # Only try to get payer info if payers_information exists
                        if std_charge.get('payers_information'):
                            payers_info = std_charge['payers_information']
                            # Handle both single dict and list of dicts
                            if isinstance(payers_info, list) and payers_info:
                                payer = payers_info[0]  # Take first payer if it's a list
                            else:
                                payer = payers_info
                            
                            if isinstance(payer, dict):
                                flat_charge['payer_name'] = str(payer.get('payer_name', ''))
                                flat_charge['plan_name'] = str(payer.get('plan_name', ''))
                        
                        # Process standard charges
                        charge_type = std_charge.get('type', '')
                        amount = std_charge.get('amount')

                        # Check for direct gross_charge field
                        gross_charge = std_charge.get('gross_charge')
                        if gross_charge is not None:
                            try:
                                flat_charge['standard_charge_gross'] = float(gross_charge)
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid gross_charge value: {gross_charge}")
                                flat_charge['standard_charge_gross'] = None

                        # Check for discounted_cash field
                        discounted_cash = std_charge.get('discounted_cash')
                        if discounted_cash is not None:
                            try:
                                flat_charge['standard_charge_negotiated_dollar'] = float(discounted_cash)
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid discounted_cash value: {discounted_cash}")
                                flat_charge['standard_charge_negotiated_dollar'] = None

                        # Check for direct minimum field
                        minimum = std_charge.get('minimum')
                        if minimum is not None:
                            try:
                                flat_charge['standard_charge_min'] = float(minimum)
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid minimum value: {minimum}")
                                flat_charge['standard_charge_min'] = None

                        # Check for direct maximum field
                        maximum = std_charge.get('maximum')
                        if maximum is not None:
                            try:
                                flat_charge['standard_charge_max'] = float(maximum)
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid maximum value: {maximum}")
                                flat_charge['standard_charge_max'] = None

                        # Process other charge types if they exist
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
                        
                        logger.info(f"Processed charge - Description: {description}, "
                                  f"Gross: {flat_charge['standard_charge_gross']}, "
                                  f"Min: {flat_charge['standard_charge_min']}, "
                                  f"Max: {flat_charge['standard_charge_max']}")
                        
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