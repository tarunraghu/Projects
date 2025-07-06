from flask import Flask, request, jsonify, session, render_template, redirect, url_for, send_file
from flask_cors import CORS
import logging
import psycopg2
from psycopg2 import pool
import json
from datetime import datetime
import os
import traceback
import csv
from pyspark.sql import SparkSession
import pandas as pd
from sqlalchemy import create_engine, text
import threading
import queue
import time
from pyspark.sql.functions import lit, round as spark_round, col, trim, upper, count, when, split, element_at, explode, array, struct, expr, regexp_replace
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id, spark_partition_id
import re
import shutil

app = Flask(__name__)  # Initialize Flask app with default template folder
CORS(app)  # Enable CORS for all routes

# Set a secret key for session management
app.secret_key = 'your-secret-key-here'  # Replace with a secure secret key in production

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure other loggers to be less verbose
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("pyspark").setLevel(logging.WARNING)

# Global task queue and results
task_queue = queue.Queue()
task_results = {}

# Add these at the top with other global variables
dump_progress = {}
dump_status = {}

class BackgroundTask:
    def __init__(self, task_id, status='PENDING', progress=0, message=''):
        self.task_id = task_id
        self.status = status
        self.progress = progress
        self.message = message
        self.result = None
        self.error = None

class SparkManager:
    _instance = None
    _spark = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_spark(self):
        try:
            if self._spark is None or self._spark._sc is None or self._spark._jsc.sc().isStopped():
                logger.info("Creating new Spark session")
                self._spark = SparkSession.builder \
                    .appName("Healthcare Data Processing") \
                    .config("spark.driver.memory", "4g") \
                    .config("spark.executor.memory", "4g") \
                    .config("spark.sql.shuffle.partitions", "10") \
                    .config("spark.network.timeout", "600s") \
                    .config("spark.executor.heartbeatInterval", "60s") \
                    .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
                    .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
                    .config("spark.jars", os.path.abspath("postgresql-42.7.2.jar")) \
                    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                    .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
                    .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY") \
                    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY") \
                    .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY") \
                    .config("spark.datasource.postgresql.url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}") \
                    .config("spark.datasource.postgresql.user", DB_CONFIG['user']) \
                    .config("spark.datasource.postgresql.password", DB_CONFIG['password']) \
                    .getOrCreate()
                
                logger.info("Spark session created successfully")
            
            return self._spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {str(e)}")
            raise

    def stop_spark(self):
        if self._spark:
            self._spark.stop()
            self._spark = None
    
    def is_spark_valid(self):
        """Check if the current Spark session is valid"""
        return self._spark is not None and self._spark._sc is not None and not self._spark._jsc.sc().isStopped()

# Database configuration
DB_CONFIG = {
    "dbname": "healthcarepoc",
    "user": "postgres",
    "password": "Consis10C!",  # Updated password
    "host": "localhost",
    "port": "5432"
}

def return_db_connection(conn):
    """Return a connection to the pool"""
    try:
        if connection_pool and conn:
            connection_pool.putconn(conn)
        else:
            conn.close()
    except Exception as e:
        logger.error(f"Error returning database connection: {str(e)}")
        if conn:
            conn.close()

# Create a connection pool
try:
    connection_pool = pool.SimpleConnectionPool(1, 20, **DB_CONFIG)
    logger.info("Successfully created database connection pool")
except Exception as e:
    logger.error(f"Error creating connection pool: {str(e)}")
    connection_pool = None

def get_db_connection():
    """Get a database connection from the pool"""
    try:
        if connection_pool:
            return connection_pool.getconn()
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        logger.error(f"Error getting database connection: {str(e)}")
        raise

def test_db_connection():
    """Test database connection and return status"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        return True, "Database connection successful"
    except Exception as e:
        error_msg = f"Database connection failed: {str(e)}"
        logger.error(error_msg)
        return False, error_msg
    finally:
        if conn:
            return_db_connection(conn)

# Test database connection on startup
db_status, db_message = test_db_connection()
logger.info(f"Database connection test: {db_message}")






def log_ingestion_details(conn, log_data):
    """Log ingestion details to hospital_log table"""
    cur = None
    try:
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO hospital_log (
            hospital_name, user_name, ingestion_type, ingestion_start_time, ingestion_end_time,
            total_records, unique_records, archived_records, status, error_message,
            file_path, processing_time_seconds
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """
        
        cur.execute(insert_query, (
            log_data['hospital_name'],
            log_data.get('user_name', 'system'),
            log_data.get('ingestion_type', 'unknown'),
            log_data['start_time'],
            log_data['end_time'],
            log_data['total_records'],
            log_data['unique_records'],
            log_data.get('archived_records', 0),
            log_data['status'],
            log_data.get('error_message'),
            log_data['file_path'],
            log_data['processing_time']
        ))
        
        conn.commit()
        logger.info(f"Successfully logged ingestion details for {log_data['hospital_name']}")
        
    except Exception as e:
        logger.error(f"Error logging ingestion details: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if cur:
            cur.close()

def load_address_data(data):
    """Load address data into PostgreSQL using Pandas"""
    try:
        logger.info("Starting to load address data using Pandas")
        df = pd.DataFrame([data])
        engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        with engine.connect() as connection:
            connection.execute(text("DELETE FROM hospital_address WHERE hospital_name = :hospital_name"), {"hospital_name": data['hospital_name']})
            connection.commit()
        df.to_sql('hospital_address', engine, if_exists='append', index=False)
        logger.info(f"Successfully loaded address data for hospital: {data['hospital_name']}")
    except Exception as e:
        logger.error(f"Error loading address data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def validate_file_path(file_path):
    """Validate and normalize file path"""
    try:
        # Remove any quotes and normalize slashes
        file_path = file_path.strip().strip('"').strip("'").replace('\\', '/')
        
        # Convert to absolute path
        abs_path = os.path.abspath(file_path)
        
        # Normalize path
        normalized_path = os.path.normpath(abs_path)
        
        logger.info(f"Original path: {file_path}")
        logger.info(f"Absolute path: {abs_path}")
        logger.info(f"Normalized path: {normalized_path}")
        
        # Check if file exists
        if not os.path.exists(normalized_path):
            raise ValueError(f"File not found: {normalized_path}")
        
        # Check if it's a file (not a directory)
        if not os.path.isfile(normalized_path):
            raise ValueError(f"Path is not a file: {normalized_path}")
        
        # Check if file is readable
        if not os.access(normalized_path, os.R_OK):
            raise ValueError(f"File is not readable: {normalized_path}")
        
        return normalized_path
        
    except Exception as e:
        logger.error(f"Error validating file path: {str(e)}")
        raise ValueError(f"Invalid file path: {str(e)}")

def detect_file_encoding(file_path):
    """Detect the file encoding by trying different common encodings"""
    encodings = [
        'utf-8', 'cp1252', 'iso-8859-1', 'latin1', 'ascii', 
        'utf-16', 'utf-32', 'windows-1250', 'windows-1252'
    ]
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                file.read()
                return encoding
        except UnicodeDecodeError:
            continue
    
    # If no encoding works, default to 'cp1252' with error handling
    return 'cp1252'

def split_csv_file(file_path, ingestion_strategy='type1'):
    """Split the CSV file into address and data files"""
    try:
        # Get the directory and filename from the path
        directory = os.path.dirname(file_path)
        base_filename = os.path.splitext(os.path.basename(file_path))[0]
        
        # Create paths for split files
        address_file = os.path.join(directory, f"{base_filename}_address.csv")
        data_file = os.path.join(directory, f"{base_filename}_charges.csv")
        
        # Detect file encoding
        file_encoding = detect_file_encoding(file_path)
        logger.info(f"Detected file encoding: {file_encoding}")
        
        # Read the original CSV file with detected encoding
        with open(file_path, 'r', encoding=file_encoding, errors='replace') as csvfile:
            reader = csv.reader(csvfile)
            all_rows = list(reader)
            
            # Validate file structure
            if len(all_rows) < 4:
                raise ValueError(f"CSV file must have at least 4 rows. Found only {len(all_rows)} rows.")
            
            # Get header row (first row) and data row (second row)
            header_row = all_rows[0]
            address_row = all_rows[1]
            
            # Check if first column is blank in both header and data
            if (not header_row[0].strip() and len(header_row) > 1 and 
                (len(address_row) == 0 or (len(address_row) > 0 and not address_row[0].strip()))):
                # Remove first column from both rows
                header_row = header_row[1:]
                if len(address_row) > 0:
                    address_row = address_row[1:]
            
            # Map header columns to expected order
            expected_columns = ['hospital_name', 'last_updated_on', 'version', 'hospital_location', 'hospital_address']
            column_mapping = {}
            
            # Clean header names and create mapping
            clean_headers = [h.strip().lower().replace(' ', '_') for h in header_row]
            
            # Try to match headers with expected columns
            for expected_col in expected_columns:
                found = False
                for idx, header in enumerate(clean_headers):
                    if (expected_col in header or 
                        (expected_col == 'hospital_name' and 'hospital' in header) or
                        (expected_col == 'last_updated_on' and 'last_updat' in header) or
                        (expected_col == 'hospital_location' and 'location' in header) or
                        (expected_col == 'hospital_address' and 'address' in header)):
                        column_mapping[expected_col] = idx
                        found = True
                        break
                if not found:
                    column_mapping[expected_col] = None
            
            # Create aligned address row using the mapping
            aligned_address = [''] * 5
            for idx, col in enumerate(expected_columns):
                if column_mapping[col] is not None and column_mapping[col] < len(address_row):
                    aligned_address[idx] = address_row[column_mapping[col]]
            
            # Write address data with UTF-8 encoding
            with open(address_file, 'w', newline='', encoding='utf-8') as addrfile:
                writer = csv.writer(addrfile)
                writer.writerow(['hospital_name', 'last_updated_on', 'version', 'hospital_location', 'hospital_address'])
                writer.writerow(aligned_address)
            
            # Write data rows with UTF-8 encoding
            with open(data_file, 'w', newline='', encoding='utf-8') as datafile:
                writer = csv.writer(datafile)
                
                # Get the header row for data (row 3, index 2)
                data_header = all_rows[2]
                if not data_header[0].strip():
                    data_header = data_header[1:]  # Remove blank first column
                clean_header = [col.strip() if col else f"_c{i}" for i, col in enumerate(data_header)]
                writer.writerow(clean_header)
                
                # Write all subsequent rows, handling blank first columns
                for row in all_rows[3:]:
                    if len(row) > 0 and not row[0].strip():
                        row = row[1:]  # Remove blank first column
                    # Clean any problematic characters
                    cleaned_row = [str(cell).replace('\x00', '').strip() for cell in row]
                    writer.writerow(cleaned_row)
            
            logger.info(f"Successfully split files into:\nAddress file: {address_file}\nData file: {data_file}")
            logger.info(f"Address data: {aligned_address}")
            return address_file, data_file
            
    except Exception as e:
        logger.error(f"Error splitting CSV file: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def read_address_file(file_path):
    """Read and parse the address file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            address_data = next(reader)  # Get the first row
            return address_data
    except Exception as e:
        logger.error(f"Error reading address file: {str(e)}")
        raise

def read_data_file_preview(file_path, num_rows=5):
    """Read and parse the data file preview"""
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            # Get header row
            headers = reader.fieldnames
            # Get first 5 rows
            preview_data = []
            total_rows = 0
            for i, row in enumerate(reader):
                total_rows += 1
                if i < num_rows:
                    preview_data.append(row)
            return headers, preview_data, total_rows
    except Exception as e:
        logger.error(f"Error reading data file preview: {str(e)}")
        raise

def process_hospital_data(df, ingestion_strategy='type1', spark=None):
    """Process the DataFrame to extract required columns"""
    try:
        from pyspark.sql.functions import round as spark_round, col, trim, upper, count, when, lit, split, element_at, explode, array, struct, expr, regexp_replace
        
        # Ensure we have a valid Spark session
        spark_manager = SparkManager.get_instance()
        if spark is None or not spark_manager.is_spark_valid():
            logger.warning("Spark session is None or stopped, getting new session from manager")
            spark = spark_manager.get_spark()
        
        # Double-check that we have a valid Spark session
        if spark is None or not spark_manager.is_spark_valid():
            raise ValueError("Unable to obtain a valid Spark session")
        
        if ingestion_strategy == 'type2':
            # Log all columns for debugging
            logger.info(f"Available columns: {df.columns}")
            
            # Find all code type columns that match the pattern code|x|type
            code_type_columns = sorted([col_name for col_name in df.columns if '|type' in col_name.lower()])
            logger.info(f"Found code type columns: {code_type_columns}")
            
            # Initialize variables for code and code_type
            code_type_col = None
            code_col = None
            
            # Try to find CPT values in each code type column
            for type_col in code_type_columns:
                # Get total count and CPT count for the column
                type_stats = df.agg(
                    count(col(type_col)).alias("total_count"),
                    count(when(trim(upper(col(type_col))).like("%CPT%"), True)).alias("cpt_count")
                ).collect()[0]
                
                total_count = type_stats["total_count"]
                cpt_count = type_stats["cpt_count"]
                
                logger.info(f"Column {type_col} - Total rows: {total_count}, CPT rows: {cpt_count}")
                
                if cpt_count > 0:
                    code_type_col = type_col
                    # Extract the number from the type column (e.g., 'code|3|type' -> '3')
                    col_num = type_col.split('|')[1]
                    # Construct the corresponding code column name
                    potential_code_col = f"code|{col_num}"
                    
                    if potential_code_col in df.columns:
                        code_col = potential_code_col
                        logger.info(f"Found matching code column: {code_col} for type column: {code_type_col}")
                        break
            
            if not code_type_col or not code_col:
                error_msg = "No code column with CPT type found in any of the code type columns"
                logger.error(error_msg)
                logger.error("Available columns: " + ", ".join(df.columns))
                raise ValueError(error_msg)
            
            # Find standard charge columns
            standard_charge_columns = {
                'gross': None,
                'min': None,
                'max': None,
                'discounted_cash': None
            }
            
            # Find all negotiated dollar columns and other related columns
            negotiated_dollar_columns = []
            negotiated_algorithm_columns = []
            estimated_amount_columns = []
            
            # Look for standard charge columns with different patterns
            for col_name in df.columns:
                col_lower = col_name.lower()
                if 'standard_charge' in col_lower:
                    if 'gross' in col_lower:
                        standard_charge_columns['gross'] = col_name
                    elif 'negotiated_dollar' in col_lower:
                        negotiated_dollar_columns.append(col_name)
                    elif 'negotiated_algorithm' in col_lower:
                        negotiated_algorithm_columns.append(col_name)
                    elif 'min' in col_lower and '|min' in col_name:
                        standard_charge_columns['min'] = col_name
                    elif 'max' in col_lower and '|max' in col_name:
                        standard_charge_columns['max'] = col_name
                    elif 'discounted_cash' in col_lower:
                        standard_charge_columns['discounted_cash'] = col_name
                elif 'estimated_amount' in col_lower:
                    estimated_amount_columns.append(col_name)
            
            logger.info(f"Found standard charge columns: {standard_charge_columns}")
            logger.info(f"Found negotiated dollar columns: {negotiated_dollar_columns}")
            
            # Create a new DataFrame with only CPT rows
            df_cpt = df.filter(trim(upper(col(code_type_col))).like("%CPT%"))
            
            # Log the count of filtered rows
            cpt_count = df_cpt.count()
            logger.info(f"Found {cpt_count} rows with CPT code type")
            
            if cpt_count == 0:
                error_msg = "No rows found with CPT code type"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # First select all necessary columns
            df_with_columns = df_cpt.select(
                'description',
                code_col,
                code_type_col,
                standard_charge_columns['gross'],
                standard_charge_columns['min'],
                standard_charge_columns['max'],
                standard_charge_columns['discounted_cash'],
                *negotiated_dollar_columns,  # Include all negotiated dollar columns
                *negotiated_algorithm_columns,  # Include all negotiated algorithm columns
                *estimated_amount_columns  # Include all estimated amount columns
            )

            # Create structs for negotiated dollar columns
            negotiated_structs = []
            for col_name in negotiated_dollar_columns:
                parts = col_name.split('|')
                if len(parts) >= 4:  # standard_charge|payer|plan|negotiated_dollar
                    payer = parts[1]
                    plan = parts[2]
                    
                    # Find corresponding columns for this payer/plan combination
                    base_payer_plan = f"{parts[0]}|{payer}|{plan}"
                    
                    # Get corresponding columns
                    negotiated_algorithm = next((c for c in negotiated_algorithm_columns if c.startswith(base_payer_plan)), None)
                    estimated_amount = next((c for c in estimated_amount_columns if c.startswith(base_payer_plan)), None)
                    
                    # Create struct with all related columns
                    negotiated_structs.append(
                        struct(
                            lit(payer).alias("payer_name"),
                            lit(plan).alias("plan_name"),
                            col(col_name).cast('decimal(20,2)').alias("standard_charge_negotiated_dollar"),
                            col(negotiated_algorithm).alias("standard_charge_negotiated_algorithm") if negotiated_algorithm else lit(None).alias("standard_charge_negotiated_algorithm"),
                            col(estimated_amount).cast('decimal(20,2)').alias("estimated_amount") if estimated_amount else lit(None).cast('decimal(20,2)').alias("estimated_amount")
                        )
                    )
            
            # Create the array of structs and explode
            if negotiated_structs:
                # Create the array of structs
                df_exploded = df_with_columns.withColumn(
                    "negotiated_info",
                    explode(array(*negotiated_structs))
                )
            else:
                # If no negotiated dollar columns found, create empty structs
                df_exploded = df_with_columns.withColumn("negotiated_info", 
                    explode(array(struct(
                        lit("").alias("payer_name"),
                        lit("").alias("plan_name"),
                        lit(0.0).cast('decimal(20,2)').alias("standard_charge_negotiated_dollar"),
                        lit(None).alias("standard_charge_negotiated_algorithm"),
                        lit(None).cast('decimal(20,2)').alias("estimated_amount")
                    ))))
            
            # Select final columns
            df_final = df_exploded.select(
                col("description"),
                col(code_col).alias("code"),
                col(code_type_col).alias("code_type"),
                col("negotiated_info.payer_name"),
                col("negotiated_info.plan_name"),
                col(standard_charge_columns['gross']).alias("standard_charge_gross"),
                col("negotiated_info.standard_charge_negotiated_dollar"),
                col(standard_charge_columns['min']).alias("standard_charge_min"),
                col(standard_charge_columns['max']).alias("standard_charge_max"),
                col(standard_charge_columns['discounted_cash']).alias("standard_charge_discounted_cash"),
                col("negotiated_info.estimated_amount")
            )
            
            # Round numeric columns to 2 decimal places
            numeric_columns = [
                'standard_charge_gross', 'standard_charge_negotiated_dollar',
                'standard_charge_min', 'standard_charge_max', 
                'standard_charge_discounted_cash', 'estimated_amount'
            ]
            
            for numeric_col in numeric_columns:
                if numeric_col in df_final.columns:
                    df_final = df_final.withColumn(numeric_col, spark_round(col(numeric_col).cast('decimal(20,2)'), 2))
            
            return df_final
            
        else:
            # Original Type 1 processing logic
            # Log all columns for debugging
            logger.info(f"Available columns: {df.columns}")
            
            # Find all code type columns that match the pattern code|x|type
            code_type_columns = sorted([col_name for col_name in df.columns if '|type' in col_name.lower()])
            logger.info(f"Found code type columns: {code_type_columns}")
            
            # Initialize variables for code and code_type
            code_type_col = None
            code_col = None
            
            # Try to find CPT values in each code type column
            for type_col in code_type_columns:
                # Get total count and CPT count for the column
                type_stats = df.agg(
                    count(col(type_col)).alias("total_count"),
                    count(when(trim(upper(col(type_col))).like("%CPT%"), True)).alias("cpt_count")
                ).collect()[0]
                
                total_count = type_stats["total_count"]
                cpt_count = type_stats["cpt_count"]
                
                logger.info(f"Column {type_col} - Total rows: {total_count}, CPT rows: {cpt_count}")
                
                if cpt_count > 0:
                    code_type_col = type_col
                    # Extract the number from the type column (e.g., 'code|3|type' -> '3')
                    col_num = type_col.split('|')[1]
                    # Construct the corresponding code column name
                    potential_code_col = f"code|{col_num}"
                    
                    if potential_code_col in df.columns:
                        code_col = potential_code_col
                        logger.info(f"Found matching code column: {code_col} for type column: {code_type_col}")
                        break
            
            if not code_type_col or not code_col:
                error_msg = "No code column with CPT type found in any of the code type columns"
                logger.error(error_msg)
                logger.error("Available columns: " + ", ".join(df.columns))
                raise ValueError(error_msg)
            
            # Find standard charge columns
            standard_charge_columns = {
                'gross': None,
                'negotiated_dollar': None,
                'min': None,
                'max': None,
                'discounted_cash': None
            }
            
            # Look for standard charge columns with different patterns
            for col_name in df.columns:
                col_lower = col_name.lower()
                if ('standard' in col_lower and 'charge' in col_lower) or ('gross' in col_lower and 'charge' in col_lower):
                    if 'gross' in col_lower:
                        standard_charge_columns['gross'] = col_name
                    elif 'negotiated_dollar' in col_lower:
                        standard_charge_columns['negotiated_dollar'] = col_name
                    elif 'min' in col_lower:
                        standard_charge_columns['min'] = col_name
                    elif 'max' in col_lower:
                        standard_charge_columns['max'] = col_name
                    elif 'discounted_cash' in col_lower:
                        standard_charge_columns['discounted_cash'] = col_name
            
            logger.info(f"Found standard charge columns: {standard_charge_columns}")
            
            # Create a new DataFrame with only CPT rows
            df_cpt = df.filter(trim(upper(col(code_type_col))).like("%CPT%"))
            
            # Log the count of filtered rows
            cpt_count = df_cpt.count()
            logger.info(f"Found {cpt_count} rows with CPT code type")
            
            if cpt_count == 0:
                error_msg = "No rows found with CPT code type"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Map the columns to the required schema
            column_mapping = {
                'description': 'description',
                code_col: 'code',
                code_type_col: 'code_type',
                'payer_name': 'payer_name',
                'plan_name': 'plan_name',
                standard_charge_columns['gross']: 'standard_charge_gross',
                standard_charge_columns['negotiated_dollar']: 'standard_charge_negotiated_dollar',
                standard_charge_columns['min']: 'standard_charge_min',
                standard_charge_columns['max']: 'standard_charge_max',
                standard_charge_columns['discounted_cash']: 'standard_charge_discounted_cash',
                'estimated_amount': 'estimated_amount'
            }
            
            # Remove None values from mapping
            column_mapping = {k: v for k, v in column_mapping.items() if k is not None}
            
            logger.info(f"Column mapping: {column_mapping}")
            
            # Create a new DataFrame with renamed columns
            select_exprs = []
            for old_col, new_col in column_mapping.items():
                select_exprs.append(col(old_col).alias(new_col))
            
            df_cpt = df_cpt.select(*select_exprs)
            
            # Round numeric columns to 2 decimal places
            numeric_columns = [
                'standard_charge_gross', 'standard_charge_negotiated_dollar',
                'standard_charge_min', 'standard_charge_max', 'standard_charge_discounted_cash'
            ]
            
            for numeric_col in numeric_columns:
                if numeric_col in df_cpt.columns:
                    df_cpt = df_cpt.withColumn(numeric_col, spark_round(col(numeric_col).cast('decimal(20,2)'), 2))
            
            return df_cpt
            
    except Exception as e:
        logger.error(f"Error processing hospital data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit-form', methods=['POST'])
def submit_form():
    """Handle form submission and validate file"""
    try:
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({
                'success': False,
                'error': 'No file uploaded'
            }), 400
            
        file = request.files['file']
        if file.filename == '':
            return jsonify({
                'success': False,
                'error': 'No file selected'
            }), 400
            
        # Get form data
        user_name = request.form.get('userName')
        file_type = request.form.get('fileType', '').lower()  # Convert to lowercase
        ingestion_strategy = request.form.get('ingestionStrategy')
        
        # Validate required fields
        if not all([user_name, file_type, ingestion_strategy]):
            return jsonify({
                'success': False,
                'error': 'Missing required fields'
            }), 400
            
        # Validate ingestion strategy
        if ingestion_strategy not in ['type1', 'type2']:
            return jsonify({
                'success': False,
                'error': f'Invalid ingestion strategy: {ingestion_strategy}'
            }), 400
            
        # Validate file type
        if file_type not in ['csv', 'json']:
            return jsonify({
                'success': False,
                'error': f'Unsupported file type: {file_type}'
            }), 400
            
        # Create upload directory if it doesn't exist
        upload_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
        os.makedirs(upload_dir, exist_ok=True)
        
        # Save the uploaded file
        file_path = os.path.join(upload_dir, file.filename)
        file.save(file_path)
        
        logger.info(f"File saved to: {file_path}")
        
        # Split the file if it's a CSV
        if file_type == 'csv':
            try:
                address_file, data_file = split_csv_file(file_path, ingestion_strategy)
                logger.info("Files split successfully")
                
                # Read the address file
                address_data = read_address_file(address_file)
                
                # Store file information in session
                session['file_path'] = file_path
                session['user_name'] = user_name
                session['file_type'] = file_type
                session['ingestion_strategy'] = ingestion_strategy
                session['address_data'] = address_data
                session['address_file'] = address_file
                session['data_file'] = data_file
                
                # Always redirect to review-address first, regardless of ingestion strategy
                return jsonify({
                    'success': True,
                    'message': 'File processed successfully',
                    'redirect': '/review-address'
                })
                
            except Exception as e:
                # Clean up uploaded file if there's an error
                if os.path.exists(file_path):
                    os.remove(file_path)
                return jsonify({
                    'success': False,
                    'error': f'Error processing file: {str(e)}'
                }), 400
        
        return jsonify({
            'success': False,
            'error': 'Only CSV files are supported at this time'
        }), 400
            
    except Exception as e:
        logger.error(f"Error processing form submission: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/review-address')
def review_address():
    # Get address data from session
    address_data = session.get('address_data')
    if not address_data:
        logger.error("No address data found in session")
        return redirect('/')
    
    # Get data file path from session
    data_file = session.get('data_file')
    if not data_file:
        logger.error("No data file found in session")
        return redirect('/')
        
    return render_template(
        'review_address.html',
        address_data=address_data
    )

@app.route('/review-charges')
def review_charges():
    # Get data file path from session
    data_file = session.get('data_file')
    if not data_file:
        logger.error("No data file found in session")
        return redirect('/')
    
    # Check if address has been confirmed
    address_confirmed = session.get('address_confirmed', False)
    if not address_confirmed:
        logger.error("Address not confirmed yet")
        return redirect('/review-address')
        
    return render_template('review_charges.html')

@app.route('/load-data')
def load_data():
    """Route to render the load data page"""
    # Get address data from session
    address_data = session.get('address_data')
    if not address_data:
        logger.error("No address data found in session")
        return redirect('/')
    
    return render_template('load_data.html')

@app.route('/preview-address')
def preview_address():
    """Endpoint to get address data for preview"""
    try:
        # Get address data from session
        address_data = session.get('address_data')
        if not address_data:
            return jsonify({
                'success': False,
                'error': 'No address data found in session'
            }), 400
            
        return jsonify({
            'success': True,
            'address_data': address_data
        })
        
    except Exception as e:
        logger.error(f"Error getting address preview: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/preview-data')
def preview_data():
    """Endpoint to get charges data for preview"""
    try:
        # Get data file path and ingestion strategy from session
        data_file = session.get('data_file')
        ingestion_strategy = session.get('ingestion_strategy', 'type1')
        
        if not data_file:
            return jsonify({
                'success': False,
                'error': 'No data file found in session'
            }), 400
        
        # Check if file exists
        if not os.path.exists(data_file):
            return jsonify({
                'success': False,
                'error': f'Data file not found: {data_file}'
            }), 400
        
        # Get Spark session from manager
        spark = SparkManager.get_instance().get_spark()
        
        # Verify Spark session is valid
        spark_manager = SparkManager.get_instance()
        if spark is None or not spark_manager.is_spark_valid():
            raise ValueError("Unable to obtain a valid Spark session")
        
        try:
            # Read CSV file with Spark
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("mode", "PERMISSIVE") \
                .option("nullValue", "NaN") \
                .option("encoding", "UTF-8") \
                .option("emptyValue", "") \
                .option("treatEmptyValuesAsNulls", "true") \
                .csv(data_file)
            
            if df is None:
                raise ValueError("Failed to read CSV file: DataFrame is None")
            
            # Get total records
            total_rows = df.count()
            if total_rows == 0:
                raise ValueError("CSV file is empty or contains no valid records")
            
            # Process data using the same transformation logic
            processed_df = process_hospital_data(df, ingestion_strategy, spark)
            
            if processed_df is None:
                raise ValueError("Failed to process hospital data: Processed DataFrame is None")
            
            # Get the transformed columns
            headers = processed_df.columns
            
            # Take first 5 rows for preview
            preview_df = processed_df.limit(5)
            
            # Convert to list of dictionaries for JSON serialization
            preview_data = []
            for row in preview_df.collect():
                row_dict = {}
                for i, value in enumerate(row):
                    # Handle None values and convert to string for JSON
                    if value is None:
                        row_dict[headers[i]] = ""
                    else:
                        row_dict[headers[i]] = str(value)
                preview_data.append(row_dict)
            
            return jsonify({
                'success': True,
                'headers': headers,
                'preview_data': preview_data,
                'total_rows': total_rows
            })
            
        finally:
            # Don't stop the Spark session here as it's managed by SparkManager
            pass
        
    except Exception as e:
        logger.error(f"Error getting data preview: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/load-address-data', methods=['POST'])
def load_address_data_route():
    """Endpoint to load address data into PostgreSQL"""
    try:
        address_data = session.get('address_data')
        if not address_data:
            return jsonify({'success': False, 'error': 'No address data found in session'}), 400
        req_data = request.get_json() or {}
        region = req_data.get('region')
        city = req_data.get('city')
        if not region or not city:
            return jsonify({'success': False, 'error': 'Both region and city are required.'}), 400
        address_data['region'] = region
        address_data['city'] = city
        logger.info(f"Loading address data: {address_data}")
        load_address_data(address_data)
        session['address_confirmed'] = True
        # Write to CSV as well
        address_file = session.get('address_file')
        if address_file:
            with open(address_file, 'w', newline='', encoding='utf-8') as addrfile:
                writer = csv.writer(addrfile)
                writer.writerow(['hospital_name', 'last_updated_on', 'version', 'hospital_location', 'hospital_address'])
                writer.writerow([
                    address_data.get('hospital_name', ''),
                    address_data.get('last_updated_on', ''),
                    address_data.get('version', ''),
                    address_data.get('hospital_location', ''),
                    address_data.get('hospital_address', '')
                ])
        return jsonify({'success': True, 'message': f'Successfully loaded address data for hospital: {address_data["hospital_name"]}', 'redirect': '/review-charges'})
    except Exception as e:
        logger.error(f"Error loading address data: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({'success': False, 'error': str(e)}), 500

def process_hospital_charges(data_file, hospital_name, task_id, user_name='system', ingestion_type='unknown', chunk_size=50000):
    """Process and load hospital charges data"""
    start_time = datetime.now()
    log_data = {
        'hospital_name': hospital_name,
        'user_name': user_name,
        'ingestion_type': ingestion_type,
        'start_time': start_time,
        'file_path': data_file,
        'total_records': 0,
        'unique_records': 0,
        'archived_records': 0,
        'status': 'PROCESSING',
        'processing_time': 0
    }
    
    try:
        task = task_results[task_id]
        task.status = 'PROCESSING'
        task.progress = 0
        task.message = 'Starting data ingestion...'
        
        
        task.progress = 10
        task.message = 'Checking for existing data...'
        
        # Check for existing data and archive if found
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            
            # First, archive existing records
            cur.execute("""
                INSERT INTO hospital_charges_archive (
                    hospital_name, description, code, code_type, 
                    payer_name, plan_name, standard_charge_gross,
                    standard_charge_negotiated_dollar, standard_charge_min,
                    standard_charge_max, standard_charge_discounted_cash,
                    estimated_amount, original_created_at, archive_reason
                )
                SELECT 
                    hospital_name, description, code, code_type,
                    payer_name, plan_name, standard_charge_gross,
                    standard_charge_negotiated_dollar, standard_charge_min,
                    standard_charge_max, standard_charge_discounted_cash,
                    estimated_amount, created_at, 'New data ingestion'
                FROM hospital_charges
                WHERE hospital_name = %s AND is_active = TRUE;
            """, (hospital_name,))
            
            # Then delete the existing records
            cur.execute("""
                DELETE FROM hospital_charges 
                WHERE hospital_name = %s AND is_active = TRUE;
            """, (hospital_name,))
            
            conn.commit()
            
            # Get count of archived records
            cur.execute("""
                SELECT COUNT(*) 
                FROM hospital_charges_archive 
                WHERE hospital_name = %s 
                AND archive_reason = 'New data ingestion'
                AND archived_at >= %s;
            """, (hospital_name, start_time))
            
            archived_count = cur.fetchone()[0]
            log_data['archived_records'] = archived_count
            task.progress = 20
            task.message = f'Successfully archived {archived_count} records'
            
        finally:
            if cur:
                cur.close()
            if conn:
                return_db_connection(conn)
        
        task.progress = 30
        task.message = 'Reading and processing data file...'

        # Get Spark session from manager
        spark = SparkManager.get_instance().get_spark()
        
        # Verify Spark session is valid
        spark_manager = SparkManager.get_instance()
        if spark is None or not spark_manager.is_spark_valid():
            raise ValueError("Unable to obtain a valid Spark session")
            
        try:
            # Read CSV file
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("mode", "PERMISSIVE") \
                .option("nullValue", "NaN") \
                .option("encoding", "UTF-8") \
                .option("emptyValue", "") \
                .option("treatEmptyValuesAsNulls", "true") \
                .csv(data_file)
            
            if df is None:
                raise ValueError("Failed to read CSV file: DataFrame is None")
            
            # Get total records
            total_records = df.count()
            if total_records == 0:
                raise ValueError("CSV file is empty or contains no valid records")
            
            log_data['total_records'] = total_records
            task.progress = 40
            task.message = f'Processing {total_records:,} records...'
            
            # Process data
            processed_df = process_hospital_data(df, ingestion_type, spark)
            
            if processed_df is None:
                raise ValueError("Failed to process hospital data: Processed DataFrame is None")
            
            # Add hospital name and timestamps
            processed_df = processed_df.withColumn('hospital_name', lit(hospital_name))
            current_time = datetime.now()
            processed_df = processed_df.withColumn('is_active', lit(True)) \
                .withColumn('created_at', lit(current_time.strftime('%Y-%m-%d %H:%M:%S')).cast('timestamp')) \
                .withColumn('updated_at', lit(current_time.strftime('%Y-%m-%d %H:%M:%S')).cast('timestamp'))
            
            task.progress = 50
            task.message = 'Removing duplicate records...'
            
            # Remove duplicates based on the unique combination of columns
            dedup_columns = ['hospital_name', 'description', 'code', 'code_type', 'payer_name', 'plan_name']
            processed_df = processed_df.dropDuplicates(dedup_columns)
            
            # Get unique records count
            unique_records = processed_df.count()
            log_data['unique_records'] = unique_records
            
            task.progress = 60
            task.message = f'Loading {unique_records:,} unique records...'
            
            # Write to PostgreSQL using the correct configuration
            processed_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}") \
                .option("dbtable", "hospital_charges") \
                .option("user", DB_CONFIG['user']) \
                .option("password", DB_CONFIG['password']) \
                .option("driver", "org.postgresql.Driver") \
                .option("truncate", "false") \
                .option("batchsize", "10000") \
                .option("isolationLevel", "READ_COMMITTED") \
                .option("stringtype", "unspecified") \
                .option("createTableColumnTypes", "standard_charge_gross NUMERIC(20,2), standard_charge_negotiated_dollar NUMERIC(20,2), standard_charge_min NUMERIC(20,2), standard_charge_max NUMERIC(20,2), standard_charge_discounted_cash NUMERIC(20,2), estimated_amount NUMERIC(20,2)") \
                .mode("append") \
                .save()
            
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            final_elapsed_str = f"{int(processing_time // 60)}m {int(processing_time % 60)}s"
            
            # Update log data
            log_data.update({
                'end_time': end_time,
                'status': 'SUCCESS',
                'processing_time': processing_time
            })
            
            # Log ingestion details
            conn = get_db_connection()
            try:
                log_ingestion_details(conn, log_data)
                # Call the cleaning function after successful ingestion
                try:
                    cur = conn.cursor()
                    cur.execute('CALL public.clean_hospital_charges()')
                    conn.commit()
                    cur.close()
                except Exception as e:
                    logger.error(f"Error calling clean_hospital_charges stored procedure: {str(e)}")
                    logger.error(traceback.format_exc())
                task.status = 'SUCCESS'
                task.result = {
                    'total_records': total_records,
                    'unique_records': unique_records,
                    'archived_records': log_data['archived_records'],
                    'processed_rows': unique_records,
                    'message': f'Successfully processed {unique_records:,} records (archived {log_data["archived_records"]:,} existing records)',
                    'elapsed_time': final_elapsed_str,
                    'average_speed': f"{unique_records/processing_time:.0f} records/second"
                }
            finally:
                return_db_connection(conn)
                
        finally:
            # Don't stop the Spark session here as it's managed by SparkManager
            pass
                
    except Exception as e:
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        error_msg = str(e)
        
        # Update log data with error
        log_data.update({
            'end_time': end_time,
            'status': 'FAILURE',
            'error_message': error_msg,
            'processing_time': processing_time
        })
        
        # Log ingestion details
        conn = get_db_connection()
        try:
            log_ingestion_details(conn, log_data)
        finally:
            return_db_connection(conn)
        
        logger.error(f"Background task error: {error_msg}")
        logger.error(traceback.format_exc())
        task.status = 'FAILURE'
        task.error = error_msg

def process_task_queue():
    """Background thread to process tasks from the queue"""
    while True:
        try:
            task = task_queue.get()
            if task:
                process_hospital_charges(task['data_file'], task['hospital_name'], task['task_id'], task['user_name'], task['ingestion_type'], task['chunk_size'])
            task_queue.task_done()
        except Exception as e:
            logger.error(f"Task queue processing error: {str(e)}")
            logger.error(traceback.format_exc())

# Start background thread for processing tasks
background_thread = threading.Thread(target=process_task_queue, daemon=True)
background_thread.start()

@app.route('/load-charges', methods=['POST'])
def load_charges():
    """Endpoint to initiate background processing of charges data"""
    conn = None
    cur = None
    try:
        # Get data file path from session
        data_file = session.get('data_file')
        if not data_file:
            return jsonify({
                'success': False,
                'error': 'No data file found in session',
                'details': 'Please upload a file first'
            }), 400
        
        # Get address data from session
        address_data = session.get('address_data')
        if not address_data or 'hospital_name' not in address_data:
            return jsonify({
                'success': False,
                'error': 'No hospital information found in session',
                'details': 'Hospital address data is required'
            }), 400
        
        # Get ingestion strategy from session
        ingestion_strategy = session.get('ingestion_strategy', 'type1')
        
        # Verify hospital exists in address table and get exact name
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT hospital_name 
                FROM hospital_address 
                WHERE hospital_name = %s
            """, (address_data['hospital_name'],))
            
            result = cur.fetchone()
            if not result:
                return jsonify({
                    'success': False,
                    'error': 'Hospital not found',
                    'details': 'Please ensure hospital address is loaded first'
                }), 400
                
            hospital_name = result[0]  # Use exact name from database
            
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            return jsonify({
                'success': False,
                'error': 'Database error',
                'details': str(e)
            }), 500
        finally:
            if cur:
                cur.close()
            if conn:
                return_db_connection(conn)
        
        # Create new task
        task_id = f"task_{int(time.time())}"
        task = BackgroundTask(task_id)
        task_results[task_id] = task
        
        # Add task to queue
        task_queue.put({
            'data_file': data_file,
            'hospital_name': hospital_name,  # Use exact name from database
            'task_id': task_id,
            'user_name': session.get('user_name', 'system'),
            'ingestion_type': ingestion_strategy,
            'chunk_size': 50000
        })
        
        return jsonify({
            'success': True,
            'message': 'Data processing started in background',
            'task_id': task_id
        })
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error initiating background process: {error_msg}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': 'System error',
            'details': error_msg
        }), 500

@app.route('/task-status/<task_id>')
def task_status(task_id):
    """Get the status of a background processing task"""
    try:
        task = task_results.get(task_id)
        if not task:
            return jsonify({
                'success': False,
                'error': 'Task not found'
            }), 404
        
        response = {
            'status': task.status,
            'progress': task.progress,
            'message': task.message
        }
        
        if task.status == 'SUCCESS':
            response['result'] = task.result
        elif task.status == 'FAILURE':
            response['error'] = task.error
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/log-error', methods=['POST'])
def log_error():
    """Endpoint to log an error in the hospital_log table"""
    try:
        # Get data from session
        hospital_name = session.get('address_data', {}).get('hospital_name')
        user_name = session.get('user_name', 'system')
        file_path = session.get('data_file', '')
        
        if not hospital_name:
            return jsonify({
                'success': False,
                'error': 'No hospital information found in session'
            }), 400
        
        # Get error message and source page from request
        data = request.get_json()
        error_message = data.get('error_message', 'Manual error logged by user')
        source_page = data.get('source_page', 'unknown')
        
        # Create log entry
        log_data = {
            'hospital_name': hospital_name,
            'user_name': user_name,
            'ingestion_type': session.get('ingestion_strategy', 'unknown'),
            'start_time': datetime.now(),
            'end_time': datetime.now(),
            'total_records': 0,
            'unique_records': 0,
            'archived_records': 0,
            'status': 'FAILURE',
            'error_message': error_message,
            'file_path': file_path,
            'processing_time': 0
        }
        
        # Log to database
        conn = get_db_connection()
        try:
            # Log the error
            log_ingestion_details(conn, log_data)
            
            # Only delete hospital address record if error is from charges page
            if source_page == 'charges':
                # Delete the hospital address record
                cur = conn.cursor()
                try:
                    cur.execute("""
                        DELETE FROM hospital_address 
                        WHERE hospital_name = %s
                    """, (hospital_name,))
                    conn.commit()
                    logger.info(f"Successfully deleted hospital address record for: {hospital_name}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Error deleting hospital address record: {str(e)}")
                    logger.error(traceback.format_exc())
                    raise
                finally:
                    if cur:
                        cur.close()
                
                return jsonify({
                    'success': True,
                    'message': 'Error logged successfully and hospital address record deleted'
                })
            else:
                return jsonify({
                    'success': True,
                    'message': 'Error logged successfully'
                })
        finally:
            return_db_connection(conn)
            
    except Exception as e:
        logger.error(f"Error logging error: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def archive_hospital_records(hospital_name):
    """Archive existing records for a hospital and remove them from the main table"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Begin transaction
        cur.execute("BEGIN;")
        
        # Get count of records to be archived
        cur.execute("""
            SELECT COUNT(*) 
            FROM hospital_charges 
            WHERE hospital_name = %s;
        """, (hospital_name,))
        
        count = cur.fetchone()[0]
        
        if count > 0:
            # Archive existing records
            cur.execute("""
                INSERT INTO hospital_charges_archive (
                    hospital_name, description, code, code_type, 
                    payer_name, plan_name, standard_charge_gross,
                    standard_charge_negotiated_dollar, standard_charge_min,
                    standard_charge_max, standard_charge_discounted_cash,
                    estimated_amount, original_created_at, archive_reason
                )
                SELECT 
                    hospital_name, description, code, code_type,
                    payer_name, plan_name, standard_charge_gross,
                    standard_charge_negotiated_dollar, standard_charge_min,
                    standard_charge_max, standard_charge_discounted_cash,
                    estimated_amount, created_at, 'New data ingestion'
                FROM hospital_charges
                WHERE hospital_name = %s;
            """, (hospital_name,))
            
            # Delete all records for this hospital from the main table
            cur.execute("""
                DELETE FROM hospital_charges 
                WHERE hospital_name = %s;
            """, (hospital_name,))
            
            # Commit transaction
            conn.commit()
            logger.info(f"Successfully archived and removed {count} records for hospital: {hospital_name}")
            return count
        else:
            # Commit transaction even if no records to archive
            conn.commit()
            logger.info(f"No records found to archive for hospital: {hospital_name}")
            return 0
            
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error archiving records for hospital {hospital_name}: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

def archive_inactive_records():
    """Move all inactive records from hospital_charges to hospital_charges_archive"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Begin transaction
        cur.execute("BEGIN;")
        
        # Get count of records to be archived
        cur.execute("""
            SELECT COUNT(*) 
            FROM hospital_charges 
            WHERE is_active = FALSE;
        """)
        result = cur.fetchone()
        count = result[0] if result else 0
        
        if count > 0:
            # Archive inactive records
            cur.execute("""
                INSERT INTO hospital_charges_archive (
                    hospital_name, description, code, code_type, 
                    payer_name, plan_name, standard_charge_gross,
                    standard_charge_negotiated_dollar, standard_charge_min,
                    standard_charge_max, original_created_at, archive_reason
                )
                SELECT 
                    hospital_name, description, code, code_type,
                    payer_name, plan_name, standard_charge_gross,
                    standard_charge_negotiated_dollar, standard_charge_min,
                    standard_charge_max, created_at, 'Inactive record cleanup'
                FROM hospital_charges
                WHERE is_active = FALSE;
            """)
            
            # Delete the archived records from the main table
            cur.execute("""
                DELETE FROM hospital_charges 
                WHERE is_active = FALSE;
            """)
            
            # Commit transaction
            conn.commit()
            
            logger.info(f"Successfully archived {count} inactive records")
            return count
        else:
            # Commit transaction even if no records to archive
            conn.commit()
            logger.info("No inactive records found to archive")
            return 0
            
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error archiving inactive records: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

@app.route('/archive-inactive', methods=['POST'])
def archive_inactive_records_route():
    """Endpoint to trigger archiving of inactive records"""
    try:
        archived_count = archive_inactive_records()
        
        return jsonify({
            'success': True,
            'message': f'Successfully archived {archived_count} inactive records'
        })
        
    except Exception as e:
        logger.error(f"Error in archive_inactive_records_route: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500




if __name__ == '__main__':
    # Start the background task processor thread
    task_processor = threading.Thread(target=process_task_queue, daemon=True)
    task_processor.start()
    
    # Start the Flask development server
    app.run(debug=True, host='0.0.0.0', port=5002)



