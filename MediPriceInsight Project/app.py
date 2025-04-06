from flask import Flask, request, jsonify, session, render_template, redirect, url_for
from flask_cors import CORS
import logging
import psycopg2
from psycopg2 import pool
import json
from datetime import datetime
from ingestion_strategies.type1_strategy import Type1IngestionStrategy
import os
import traceback
import csv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd
from sqlalchemy import create_engine, text
import threading
import queue
import time
from datetime import datetime, timedelta
from pyspark.sql.functions import lit, round as spark_round
from io import StringIO
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id, spark_partition_id

app = Flask(__name__)  # Initialize Flask app with default template folder
CORS(app)  # Enable CORS for all routes

# Set a secret key for session management
app.secret_key = 'your-secret-key-here'  # Replace with a secure secret key in production

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Global task queue and results
task_queue = queue.Queue()
task_results = {}

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
        if self._spark is None or self._spark._jsc.sc().isStopped():
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
                .getOrCreate()
        return self._spark

    def stop_spark(self):
        if self._spark:
            self._spark.stop()
            self._spark = None

# Database configuration
DB_CONFIG = {
    "dbname": "healthcarepoc",
    "user": "postgres",
    "password": "Consis10C!",
    "host": "localhost",
    "port": "5432"
}

# Create a connection pool
try:
    connection_pool = pool.SimpleConnectionPool(1, 20, **DB_CONFIG)
except Exception as e:
    logger.error(f"Error creating connection pool: {str(e)}")
    connection_pool = None

def get_db_connection():
    """Get a database connection from the pool"""
    if connection_pool:
        return connection_pool.getconn()
    return psycopg2.connect(**DB_CONFIG)

def return_db_connection(conn):
    """Return a connection to the pool"""
    if connection_pool and conn:
        connection_pool.putconn(conn)
    else:
        conn.close()

def create_log_table():
    """Create the hospital_log table if it doesn't exist"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS hospital_log (
            id SERIAL PRIMARY KEY,
            user_name VARCHAR(255),
            file_type VARCHAR(50),
            file_path TEXT,
            ingestion_strategy VARCHAR(50),
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ingestion_status BOOLEAN DEFAULT FALSE,
            error_message TEXT
        );
        """
        
        cur.execute(create_table_query)
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error creating log table: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if cur:
            cur.close()
        return_db_connection(conn)

def create_hospital_data_table():
    """Create the hospital_data table if it doesn't exist"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Drop the existing table if it exists
        cur.execute("DROP TABLE IF EXISTS hospital_data;")
        
        create_table_query = """
        CREATE TABLE hospital_data (
            id SERIAL PRIMARY KEY,
            description VARCHAR(255),
            code VARCHAR(50),
            code_type VARCHAR(50),
            payer_name VARCHAR(255),
            plan_name VARCHAR(255),
            standard_charge_gross NUMERIC,
            standard_charge_negotiated_dollar NUMERIC,
            standard_charge_min NUMERIC,
            standard_charge_max NUMERIC
        );
        """
        
        cur.execute(create_table_query)
        conn.commit()
        
        logger.info("Successfully created hospital_data table with new schema")
        
    except Exception as e:
        logger.error(f"Error creating hospital_data table: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

def create_hospital_address_table():
    """Create the hospital_address table if it doesn't exist"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS hospital_address (
            id SERIAL PRIMARY KEY,
            hospital_name VARCHAR(255) UNIQUE,
            last_updated_on VARCHAR(50),
            version VARCHAR(50),
            hospital_location VARCHAR(255),
            hospital_address VARCHAR(255)
        );
        """
        
        cur.execute(create_table_query)
        conn.commit()
        
        logger.info("Successfully ensured hospital_address table exists")
        
    except Exception as e:
        logger.error(f"Error creating hospital_address table: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

def create_hospital_charges_table():
    """Create the hospital_charges table with standardized schema"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Create the main charges table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS hospital_charges (
            id SERIAL PRIMARY KEY,
            hospital_name TEXT,
            description TEXT,
            code VARCHAR(50),
            code_type VARCHAR(50),
            payer_name VARCHAR(255),
            plan_name VARCHAR(255),
            standard_charge_gross NUMERIC(20,2),
            standard_charge_negotiated_dollar NUMERIC(20,2),
            standard_charge_min NUMERIC(20,2),
            standard_charge_max NUMERIC(20,2),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        conn.commit()
        logger.info("Successfully created hospital_charges table")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating hospital_charges table: {str(e)}")
        raise
    finally:
        if conn:
            return_db_connection(conn)

def create_hospital_charges_archive_table():
    """Create the hospital_charges_archive table with standardized schema"""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # Create the archive table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS hospital_charges_archive (
            id SERIAL PRIMARY KEY,
            hospital_name TEXT,
            description TEXT,
            code VARCHAR(50),
            code_type VARCHAR(50),
            payer_name VARCHAR(255),
            plan_name VARCHAR(255),
            standard_charge_gross NUMERIC(20,2),
            standard_charge_negotiated_dollar NUMERIC(20,2),
            standard_charge_min NUMERIC(20,2),
            standard_charge_max NUMERIC(20,2),
            original_created_at TIMESTAMP,
            archive_reason TEXT,
            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        conn.commit()
        logger.info("Successfully created hospital_charges_archive table")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating hospital_charges_archive table: {str(e)}")
        raise
    finally:
        if conn:
            return_db_connection(conn)

def create_hospital_log_table():
    """Create the hospital_log table if it doesn't exist"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS hospital_log (
            id SERIAL PRIMARY KEY,
            hospital_name TEXT,
            user_name VARCHAR(255),
            ingestion_type VARCHAR(50),
            ingestion_start_time TIMESTAMP,
            ingestion_end_time TIMESTAMP,
            total_records INTEGER,
            unique_records INTEGER,
            archived_records INTEGER,
            status VARCHAR(50),
            error_message TEXT,
            file_path TEXT,
            processing_time_seconds NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cur.execute(create_table_query)
        conn.commit()
        
        logger.info("Successfully ensured hospital_log table exists with correct schema")
        
    except Exception as e:
        logger.error(f"Error creating hospital_log table: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

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
        
        # Create a DataFrame from the address data
        df = pd.DataFrame([data])
        
        # Create SQLAlchemy engine for PostgreSQL
        engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        
        # First, try to delete existing record for this hospital using SQLAlchemy text()
        with engine.connect() as connection:
            connection.execute(text("DELETE FROM hospital_address WHERE hospital_name = :hospital_name"), 
                            {"hospital_name": data['hospital_name']})
            connection.commit()
        
        # Write DataFrame to PostgreSQL
        df.to_sql('hospital_address', engine, if_exists='append', index=False)
        
        logger.info(f"Successfully loaded address data for hospital: {data['hospital_name']}")
        
    except Exception as e:
        logger.error(f"Error loading address data: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def load_hospital_data(file_path):
    """Load data from hospital data CSV file into hospital_data table using PySpark"""
    spark = None
    try:
        logger.info(f"Starting to load hospital data from file: {file_path}")
        
        # Create Spark session if not exists
        spark = SparkSession.builder \
            .appName("Healthcare Data Processing") \
            .config("spark.jars", "postgresql-42.7.2.jar") \
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.jars", os.path.abspath("postgresql-42.7.2.jar")) \
            .getOrCreate()
        
        logger.info("Spark session created successfully")
        
        # Read the CSV file with first row as header and handle empty column names
        logger.info("Attempting to read CSV file...")
        df = spark.read.option("header", "true") \
            .option("encoding", "UTF-8") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .option("emptyValue", "") \
            .option("treatEmptyValuesAsNulls", "true") \
            .csv(file_path)
        
        # Rename empty column names to _cX format
        new_columns = []
        for i, col in enumerate(df.columns):
            if not col or col.strip() == "":
                new_columns.append(f"_c{i}")
            else:
                new_columns.append(col)
        
        # Rename columns
        for old_col, new_col in zip(df.columns, new_columns):
            df = df.withColumnRenamed(old_col, new_col)
        
        logger.info("CSV file read successfully")
        
        # Get total rows count
        total_rows = df.count()
        logger.info(f"Total rows loaded: {total_rows}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading hospital data: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if spark:
            try:
                spark.stop()
            except Exception as e:
                logger.error(f"Error closing Spark session: {str(e)}")

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
        from pyspark.sql.functions import round as spark_round
        
        if ingestion_strategy == 'type2':
            if spark is None:
                raise ValueError("Spark session is required for Type 2 ingestion")
                
            # Log original columns for debugging
            logger.info(f"Original columns in DataFrame: {df.columns}")
            
            # Find all standard charge columns
            standard_charge_columns = {
                'gross': None,
                'min': None,
                'max': None
            }
            
            # Find the exact column names for standard charges (case-insensitive)
            for col in df.columns:
                col_lower = col.lower()
                if 'standard_charge' in col_lower:
                    if 'gross' in col_lower:
                        standard_charge_columns['gross'] = col
                    elif 'min' in col_lower:
                        standard_charge_columns['min'] = col
                    elif 'max' in col_lower:
                        standard_charge_columns['max'] = col
            
            logger.info(f"Found standard charge columns: {standard_charge_columns}")
            
            # Find code and code type columns
            code_columns = [col for col in df.columns if 'code' in col.lower() and not col.lower().endswith('type')]
            code_type_columns = [col for col in df.columns if col.lower().endswith('type')]
            
            logger.info(f"Found code columns: {code_columns}")
            logger.info(f"Found code type columns: {code_type_columns}")
            
            # Find the code column with CPT type
            selected_code_col = None
            selected_type_col = None
            
            for type_col in code_type_columns:
                cpt_count = df.filter(df[type_col].rlike('(?i)CPT')).count()
                if cpt_count > 0:
                    selected_type_col = type_col
                    # Find corresponding code column
                    base_name = type_col.rsplit('|', 1)[0] if '|' in type_col else type_col.replace('type', '').strip()
                    matching_code_cols = [col for col in code_columns if base_name in col]
                    if matching_code_cols:
                        selected_code_col = matching_code_cols[0]
                        break
            
            if not selected_code_col or not selected_type_col:
                error_msg = "No code column with CPT type found"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"Selected code column: {selected_code_col}")
            logger.info(f"Selected type column: {selected_type_col}")
            
            # Create the base query for each payer/plan combination
            base_query = f"""
                SELECT 
                    description,
                    `{selected_code_col}` as code,
                    `{selected_type_col}` as code_type
            """
            
            # Add standard charge columns to base query with rounding
            if standard_charge_columns['gross']:
                base_query += f",\n    ROUND(CAST(`{standard_charge_columns['gross']}` as DECIMAL(20,2)), 2) as standard_charge_gross"
            else:
                base_query += ",\n    CAST(NULL as DECIMAL(20,2)) as standard_charge_gross"
                
            if standard_charge_columns['min']:
                base_query += f",\n    ROUND(CAST(`{standard_charge_columns['min']}` as DECIMAL(20,2)), 2) as standard_charge_min"
            else:
                base_query += ",\n    CAST(NULL as DECIMAL(20,2)) as standard_charge_min"
                
            if standard_charge_columns['max']:
                base_query += f",\n    ROUND(CAST(`{standard_charge_columns['max']}` as DECIMAL(20,2)), 2) as standard_charge_max"
            else:
                base_query += ",\n    CAST(NULL as DECIMAL(20,2)) as standard_charge_max"
            
            # Find all payer/plan combinations with negotiated_dollar
            payer_plan_columns = []
            for col in df.columns:
                if 'standard_charge' in col.lower() and 'negotiated_dollar' in col.lower():
                    parts = col.split('|')
                    if len(parts) >= 4:
                        payer_plan_columns.append({
                            'column': col,
                            'payer': parts[1],
                            'plan': parts[2]
                        })
            
            logger.info(f"Found payer/plan columns: {payer_plan_columns}")
            
            # Create union queries for each payer/plan combination
            union_queries = []
            for pp in payer_plan_columns:
                query = f"""
                    {base_query},
                    '{pp['payer']}' as payer_name,
                    '{pp['plan']}' as plan_name,
                    ROUND(CAST(`{pp['column']}` as DECIMAL(20,2)), 2) as standard_charge_negotiated_dollar
                FROM __this__
                WHERE `{pp['column']}` IS NOT NULL
                """
                union_queries.append(query)
            
            if not union_queries:
                error_msg = "No valid payer/plan combinations found with negotiated dollar amounts"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Combine all queries
            final_query = " UNION ALL ".join(union_queries)
            logger.info(f"Generated final SQL query: {final_query}")
            
            # Create temp view and execute query
            df.createOrReplaceTempView("__this__")
            df_final = spark.sql(final_query)
            
            return df_final
            
        else:
            # Original Type 1 processing logic
            code_columns = sorted([col for col in df.columns if col.startswith('code|') and not col.endswith('|type')])
            code_type_columns = sorted([col for col in df.columns if col.endswith('|type')])
            
            logger.info(f"Found code columns: {code_columns}")
            logger.info(f"Found code type columns: {code_type_columns}")
            
            # Initialize variables for code and code_type
            selected_code_col = None
            selected_code_type_col = None
            
            # Find the first code column that has CPT type
            for code_col in code_columns:
                base_name = code_col.split('|')[1]  # Extract the number part (e.g., '1' from 'code|1')
                type_col = f"code|{base_name}|type"
                
                if type_col in code_type_columns:
                    # Check if this column has 'CPT' value
                    cpt_count = df.filter(df[type_col].rlike('(?i)CPT')).count()
                    logger.info(f"Column {type_col} has {cpt_count} CPT values")
                    
                    if cpt_count > 0:
                        selected_code_col = code_col
                        selected_code_type_col = type_col
                        logger.info(f"Selected code column: {selected_code_col} with type column: {selected_code_type_col}")
                        break
            
            if not selected_code_col or not selected_code_type_col:
                error_msg = "No code column with CPT type found"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Select and rename required columns
            required_columns = {
                'description': 'description',
                selected_code_col: 'code',
                selected_code_type_col: 'code_type',
                'payer_name': 'payer_name',
                'plan_name': 'plan_name',
                'standard_charge|gross': 'standard_charge_gross',
                'standard_charge|negotiated_dollar': 'standard_charge_negotiated_dollar',
                'standard_charge|min': 'standard_charge_min',
                'standard_charge|max': 'standard_charge_max'
            }
            
            # Filter out None keys (in case some columns weren't found)
            required_columns = {k: v for k, v in required_columns.items() if k is not None}
            
            # Create a new DataFrame with only CPT rows and required columns
            df_cpt = df.filter(df[selected_code_type_col].rlike('(?i)CPT'))
            
            if df_cpt.count() == 0:
                error_msg = "No rows found with CPT code type"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Select and rename columns
            for old_col, new_col in required_columns.items():
                if old_col in df_cpt.columns:
                    df_cpt = df_cpt.withColumnRenamed(old_col, new_col)
            
            # Select only the required columns that exist
            existing_columns = [col for col in required_columns.values() if col in df_cpt.columns]
            df_cpt = df_cpt.select(*existing_columns)
            
            # Round numeric columns to 2 decimal places
            numeric_columns = [
                'standard_charge_gross', 'standard_charge_negotiated_dollar',
                'standard_charge_min', 'standard_charge_max'
            ]
            
            for col in numeric_columns:
                if col in df_cpt.columns:
                    df_cpt = df_cpt.withColumn(col, spark_round(df_cpt[col].cast('decimal(20,2)'), 2))
            
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
        file_type = request.form.get('fileType')
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
                
                # Redirect based on ingestion strategy
                if ingestion_strategy == 'type1':
                    return jsonify({
                        'success': True,
                        'message': 'File processed successfully',
                        'redirect': '/review-address'
                    })
                else:
                    return jsonify({
                        'success': True,
                        'message': 'File processed successfully',
                        'redirect': '/review-charges'
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

@app.route('/ingest-data', methods=['POST'])
def ingest_data():
    """Endpoint to ingest data into PostgreSQL"""
    try:
        data = request.get_json()
        if not data or 'confirm' not in data:
            return jsonify({
                'success': False,
                'error': 'Invalid request data'
            })
        
        if 'hospital_preview' not in session:
            return jsonify({
                'success': False,
                'error': 'No data available for ingestion'
            })
        
        preview_data = session['hospital_preview']
        file_path = session.get('file_path')
        
        if not file_path:
            return jsonify({
                'success': False,
                'error': 'File path not found in session'
            })
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("Healthcare Data Processing") \
            .config("spark.jars", "postgresql-42.7.2.jar") \
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .getOrCreate()
        
        try:
            # Read the CSV file
            df = spark.read \
                .option("header", "true") \
                .option("encoding", "UTF-8") \
                .option("skipRows", 1) \
                .csv(file_path)
            
            # Apply the same column mapping as in preview
            column_mapping = preview_data['column_mapping']
            select_exprs = []
            for file_col in df.columns:
                if file_col in column_mapping:
                    select_exprs.append(f"{file_col} as {column_mapping[file_col]}")
                else:
                    select_exprs.append(file_col)
            
            df = df.selectExpr(*select_exprs)
            
            # Write to PostgreSQL using the correct configuration
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}") \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", "hospital_data") \
                .option("user", DB_CONFIG['user']) \
                .option("password", DB_CONFIG['password']) \
                .mode("append") \
                .save()
            
            # Clear session data
            session.pop('hospital_preview', None)
            session.pop('file_path', None)
            
            return jsonify({
                'success': True,
                'message': 'Data ingested successfully'
            })
            
        finally:
            spark.stop()
            
    except Exception as e:
        logger.error(f"Error ingesting data: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/load-main-data', methods=['POST'])
def load_main_data():
    conn = None
    cur = None
    try:
        # Create hospital_data table if it doesn't exist
        create_hospital_data_table()
        
        # Get JSON data from request
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'No JSON data received'
            }), 400
            
        logger.info(f"Received load main data request: {data}")
        
        # Get data file path from session
        data_file = session.get('data_file')
        if not data_file:
            return jsonify({
                'success': False,
                'error': 'No data file found in session'
            }), 400
            
        normalized_path = validate_file_path(data_file)
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("Healthcare Data Processing") \
            .config("spark.jars", "postgresql-42.7.2.jar") \
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.jars", os.path.abspath("postgresql-42.7.2.jar")) \
            .getOrCreate()
        
        try:
            # Read the CSV file with first row as header
            logger.info(f"Reading data file: {normalized_path}")
            df = spark.read \
                .option("header", "true") \
                .option("encoding", "UTF-8") \
                .option("inferSchema", "true") \
                .option("mode", "PERMISSIVE") \
                .option("emptyValue", "") \
                .option("treatEmptyValuesAsNulls", "true") \
                .csv(normalized_path)
            
            # Get total rows count
            total_rows = df.count()
            logger.info(f"Total rows in DataFrame: {total_rows}")
            
            # Write to PostgreSQL using the correct configuration
            logger.info("Writing DataFrame to PostgreSQL...")
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}") \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", "hospital_data") \
                .option("user", DB_CONFIG['user']) \
                .option("password", DB_CONFIG['password']) \
                .mode("append") \
                .save()
            
            logger.info("Data successfully written to PostgreSQL")
            
            # Clear session data
            session.pop('data_file', None)
            
            return jsonify({
                'success': True,
                'message': f'Successfully loaded {total_rows} rows into database'
            })
            
        finally:
            spark.stop()
            
    except Exception as e:
        logger.error(f"Error loading main data: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Rollback transaction if connection exists
        if conn:
            conn.rollback()
        
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        # Close cursor and return connection to pool
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

@app.route('/preview-hospital-data')
def preview_hospital_data():
    """Endpoint to get preview of hospital data"""
    try:
        # Get preview data from session
        preview_data = session.get('hospital_preview')
        if not preview_data:
            return jsonify({
                'success': False,
                'error': 'No preview data available. Please upload a file first.'
            })
        
        logger.info(f"Preview data from session: {preview_data}")
        
        # Ensure all required keys exist
        if not all(key in preview_data for key in ['data', 'total_rows', 'original_columns']):
            return jsonify({
                'success': False,
                'error': 'Incomplete preview data in session'
            })
        
        return jsonify({
            'success': True,
            'preview_data': preview_data['data'],
            'total_rows': preview_data['total_rows'],
            'original_columns': preview_data['original_columns']
        })
        
    except Exception as e:
        logger.error(f"Error getting preview data: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/load-split-files', methods=['POST'])
def load_split_files():
    try:
        # Create tables if they don't exist
        create_hospital_address_table()
        create_hospital_data_table()
        
        # Load data from split files
        address_file = 'USMD_Arlington_address.csv'
        data_file = 'USMD_Arlington_data.csv'
        
        load_address_data(address_file)
        load_hospital_data(data_file)
        
        return jsonify({
            'success': True,
            'message': 'Successfully loaded data from split files'
        })
        
    except Exception as e:
        logger.error(f"Error loading split files: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/preview-data')
def preview_data():
    """Endpoint to get preview of the data file"""
    spark_manager = None
    try:
        # Get data file path from session
        data_file = session.get('data_file')
        if not data_file:
            return jsonify({
                'success': False,
                'error': 'No data file found in session',
                'details': 'Please upload a file first',
                'technical_details': 'Session data_file key is missing or None'
            }), 400
            
        if not os.path.exists(data_file):
            return jsonify({
                'success': False,
                'error': 'File not found',
                'details': 'The uploaded file could not be found on the server',
                'technical_details': f'File path {data_file} does not exist'
            }), 404
            
        # Get ingestion strategy from session
        ingestion_strategy = session.get('ingestion_strategy', 'type1')
            
        # Get Spark session
        spark_manager = SparkManager.get_instance()
        spark = spark_manager.get_spark()
            
        try:
            # Read the CSV file
            logger.info(f"Reading file: {data_file}")
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("mode", "PERMISSIVE") \
                .option("nullValue", "NaN") \
                .csv(data_file)
            
            # Log the columns found in the file
            original_columns = df.columns
            logger.info(f"Original columns found in file: {original_columns}")
            
            try:
                processed_df = process_hospital_data(df, ingestion_strategy, spark)
                processed_columns = processed_df.columns
                logger.info(f"Processed columns: {processed_columns}")
                
                # Convert to Pandas and handle NaN values
                pandas_df = processed_df.limit(5).toPandas()
                
                # Replace NaN values with None for JSON serialization
                preview_data = pandas_df.where(pandas_df.notna(), None).to_dict('records')
                
                # Get column headers
                headers = processed_df.columns
                
                # Get total rows
                total_rows = processed_df.count()
                
                if not headers or not preview_data:
                    missing_columns = []
                    if ingestion_strategy == 'type2':
                        required_columns = ['description', 'code', 'payer_name', 'plan_name', 
                                         'standard_charge_gross', 'standard_charge_negotiated_dollar']
                    else:
                        required_columns = ['description', 'code', 'payer_name', 'plan_name', 
                                         'standard_charge_gross', 'standard_charge_negotiated_dollar',
                                         'standard_charge_min', 'standard_charge_max']
                    
                    for col in required_columns:
                        if col not in headers:
                            missing_columns.append(col)
                    
                    return jsonify({
                        'success': False,
                        'error': 'Missing required columns',
                        'details': 'The file is missing some required columns',
                        'technical_details': 'Data processing resulted in empty DataFrame',
                        'available_columns': list(original_columns),
                        'missing_columns': missing_columns,
                        'total_rows': total_rows
                    }), 400
                
                return jsonify({
                    'success': True,
                    'headers': headers,
                    'preview_data': preview_data,
                    'total_rows': total_rows,
                    'original_columns': list(original_columns),
                    'processed_columns': list(processed_columns)
                })
                
            except Exception as process_error:
                logger.error(f"Error processing data: {str(process_error)}")
                logger.error(traceback.format_exc())
                return jsonify({
                    'success': False,
                    'error': 'Error processing data',
                    'details': str(process_error),
                    'technical_details': traceback.format_exc(),
                    'available_columns': list(original_columns)
                }), 500
            
        except Exception as spark_error:
            error_msg = str(spark_error)
            logger.error(f"Spark processing error: {error_msg}")
            logger.error(traceback.format_exc())
            return jsonify({
                'success': False,
                'error': 'Error reading file',
                'details': 'Failed to read the CSV file',
                'technical_details': error_msg
            }), 500
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"General error in preview_data: {error_msg}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': 'System error',
            'details': 'An unexpected error occurred',
            'technical_details': error_msg
        }), 500
    finally:
        # Don't stop Spark here, let it be managed by the SparkManager
        pass

@app.route('/load-address-data', methods=['POST'])
def load_address_data_route():
    """Endpoint to load address data into PostgreSQL"""
    try:
        # Get address data from session
        address_data = session.get('address_data')
        if not address_data:
            return jsonify({
                'success': False,
                'error': 'No address data found in session'
            }), 400
            
        logger.info(f"Loading address data: {address_data}")
        
        # Create hospital_address table if it doesn't exist
        create_hospital_address_table()
        
        # Load the address data
        load_address_data(address_data)
        
        return jsonify({
            'success': True,
            'message': f'Successfully loaded address data for hospital: {address_data["hospital_name"]}'
        })
        
    except Exception as e:
        logger.error(f"Error loading address data: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def process_chunks(df, chunk_size=50000):
    """Process DataFrame in chunks and insert into database using PySpark JDBC"""
    total_rows = df.count()
    num_chunks = (total_rows + chunk_size - 1) // chunk_size
    
    # Calculate number of partitions based on data size and chunk size
    num_partitions = min(num_chunks, 200)  # Cap at 200 partitions to avoid over-partitioning
    
    # Repartition the DataFrame for better distribution
    df = df.repartition(num_partitions)
    
    # Add row numbers with proper partitioning
    window_spec = Window.partitionBy(spark_partition_id()).orderBy(monotonically_increasing_id())
    
    # Add partition ID and row number
    df_with_row_num = df.withColumn("partition_id", spark_partition_id()) \
                        .withColumn("row_num", row_number().over(window_spec))
    
    # JDBC connection properties
    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    connection_properties = {
        "user": DB_CONFIG['user'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver",
        "batchsize": "10000"  # Adjust batch size for optimal performance
    }
    
    for i in range(num_chunks):
        start_idx = i * chunk_size + 1  # 1-based row numbers
        end_idx = min((i + 1) * chunk_size, total_rows)
        
        try:
            # Get chunk of data using row numbers and partition ID
            chunk = df_with_row_num.filter(
                (df_with_row_num.row_num >= start_idx) & 
                (df_with_row_num.row_num <= end_idx)
            ).drop("row_num", "partition_id")
            
            # Write chunk directly to database using JDBC
            chunk.write \
                .mode("append") \
                .jdbc(
                    url=jdbc_url,
                    table="hospital_charges",
                    properties=connection_properties
                )
            
            logger.info(f"Successfully inserted chunk {i+1}/{num_chunks} ({end_idx-start_idx+1} records)")
            
        except Exception as e:
            logger.error(f"Error inserting chunk {i+1}: {str(e)}")
            raise

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
        task.message = 'Starting data processing...'
        
        # Create required tables (same tables for both Type 1 and Type 2)
        create_hospital_charges_table()
        create_hospital_charges_archive_table()
        create_hospital_log_table()
        
        # Check for existing hospital data and handle archival
        conn = get_db_connection()
        archived_count = 0
        
        try:
            cur = conn.cursor()
            
            # Begin transaction
            cur.execute("BEGIN;")
            
            # First verify hospital exists in address table
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM hospital_address 
                    WHERE hospital_name = %s
                );
            """, (hospital_name,))
            
            if not cur.fetchone()[0]:
                raise ValueError(f"Hospital '{hospital_name}' not found in address table. Please load hospital address data first.")
            
            # Check for existing active records
            cur.execute("""
                SELECT COUNT(*) 
                FROM hospital_charges
                WHERE hospital_name = %s AND is_active = TRUE;
            """, (hospital_name,))
            
            existing_records = cur.fetchone()[0]
            
            if existing_records > 0:
                task.message = f'Found {existing_records:,} existing active records for {hospital_name}...'
                logger.info(f"Found {existing_records} existing active records for hospital: {hospital_name}")
                
                # Archive existing records
                archive_query = """
                WITH records_to_archive AS (
                    SELECT 
                        hospital_name, description, code, code_type,
                        payer_name, plan_name,
                        standard_charge_gross,
                        standard_charge_negotiated_dollar,
                        standard_charge_min,
                        standard_charge_max,
                        created_at
                    FROM hospital_charges 
                    WHERE hospital_name = %s AND is_active = TRUE
                ),
                archive_insert AS (
                    INSERT INTO hospital_charges_archive (
                        hospital_name, description, code, code_type,
                        payer_name, plan_name,
                        standard_charge_gross,
                        standard_charge_negotiated_dollar,
                        standard_charge_min,
                        standard_charge_max,
                        original_created_at,
                        archive_reason
                    )
                    SELECT 
                        hospital_name, description, code, code_type,
                        payer_name, plan_name,
                        standard_charge_gross,
                        standard_charge_negotiated_dollar,
                        standard_charge_min,
                        standard_charge_max,
                        created_at,
                        'Replaced by new data upload'
                    FROM records_to_archive
                ),
                deactivate_records AS (
                    UPDATE hospital_charges 
                    SET 
                        is_active = FALSE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE hospital_name = %s AND is_active = TRUE
                    RETURNING 1
                )
                SELECT COUNT(*) FROM deactivate_records;
                """
                
                # Execute archive query
                cur.execute(archive_query, (hospital_name, hospital_name))
                archived_count = cur.fetchone()[0]
                
                task.message = f'Successfully archived {archived_count:,} records for {hospital_name}...'
                logger.info(f"Successfully archived {archived_count} records for hospital: {hospital_name}")
                
                # Update log data
                log_data['archived_records'] = archived_count
            else:
                task.message = f'No existing active records found for {hospital_name}, proceeding with new data ingestion...'
                logger.info(f"No existing active records found for hospital: {hospital_name}")
            
            # Commit transaction
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during hospital data check/archival: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                return_db_connection(conn)

        # Create Spark session with memory configuration and JDBC driver
        spark = SparkSession.builder \
            .appName("Healthcare Data Processing") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.jars", os.path.abspath("postgresql-42.7.2.jar")) \
            .getOrCreate()
            
        try:
            # Read and process the CSV file
            task.message = 'Reading CSV file...'
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("mode", "PERMISSIVE") \
                .option("nullValue", "NaN") \
                .csv(data_file)
            
            # Get total records before deduplication
            total_records = df.count()
            log_data['total_records'] = total_records
            task.message = f'Found {total_records:,} total records in file...'
            
            task.message = 'Processing data...'
            processed_df = process_hospital_data(df, ingestion_type, spark)
            
            # Add hospital name column and active flag
            processed_df = processed_df.withColumn('hospital_name', lit(hospital_name))
            
            # Add timestamp columns and active flag with proper datetime format
            current_time = datetime.now()
            processed_df = processed_df.withColumn('is_active', lit(True)) \
                .withColumn('created_at', lit(current_time.strftime('%Y-%m-%d %H:%M:%S')).cast('timestamp')) \
                .withColumn('updated_at', lit(current_time.strftime('%Y-%m-%d %H:%M:%S')).cast('timestamp'))
            
            # Remove duplicates based on key fields
            task.message = 'Removing duplicate records...'
            dedup_columns = ['hospital_name', 'description', 'code', 'code_type', 'payer_name', 'plan_name']
            processed_df = processed_df.dropDuplicates(dedup_columns)
            
            unique_records = processed_df.count()
            log_data['unique_records'] = unique_records
            task.message = f'Found {unique_records:,} unique records to process...'
            
            # Process in chunks using the new function
            process_chunks(processed_df, chunk_size)
            
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            # Format final elapsed time
            final_minutes = int(processing_time // 60)
            final_seconds = int(processing_time % 60)
            final_elapsed_str = f"{final_minutes}m {final_seconds}s"
            
            # Final progress update
            task.progress = 100
            task.message = (
                f'Processing complete!\n'
                f'Archived: {archived_count:,} records\n'
                f'Processed: {unique_records:,} new records\n'
                f'Elapsed time: {final_elapsed_str}'
            )
            
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
            finally:
                return_db_connection(conn)
            
            task.status = 'SUCCESS'
            task.result = {
                'total_records': total_records,
                'unique_records': unique_records,
                'processed_rows': unique_records,
                'archived_rows': archived_count,
                'message': f'Successfully processed {unique_records:,} records',
                'elapsed_time': final_elapsed_str,
                'average_speed': f"{unique_records/processing_time:.0f} records/second"
            }
            
        finally:
            if spark:
                spark.stop()
                
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
        
        # Verify hospital exists in address table and get exact name
        conn = None
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
            'ingestion_type': session.get('ingestion_strategy', 'unknown'),
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

def create_hospital_charges_type2_table():
    """Create the hospital_charges_type2 table for Type 2 ingestion"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Drop the table if it exists to ensure clean schema
        cur.execute("DROP TABLE IF EXISTS hospital_charges_type2;")
        
        create_table_query = """
        CREATE TABLE hospital_charges_type2 (
            id SERIAL PRIMARY KEY,
            hospital_name TEXT,
            description TEXT,
            code VARCHAR(50),
            code_type VARCHAR(50),
            payer_name VARCHAR(255),
            plan_name VARCHAR(255),
            standard_charge_gross NUMERIC(38,18),
            standard_charge_negotiated_dollar NUMERIC(38,18),
            standard_charge_min NUMERIC(38,18),
            standard_charge_max NUMERIC(38,18),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cur.execute(create_table_query)
        conn.commit()
        
        logger.info("Successfully created hospital_charges_type2 table")
        
    except Exception as e:
        logger.error(f"Error creating hospital_charges_type2 table: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

def create_hospital_charges_type2_archive_table():
    """Create the hospital_charges_type2_archive table for Type 2 ingestion"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Drop the table if it exists to ensure clean schema
        cur.execute("DROP TABLE IF EXISTS hospital_charges_type2_archive;")
        
        create_table_query = """
        CREATE TABLE hospital_charges_type2_archive (
            id SERIAL PRIMARY KEY,
            hospital_name TEXT,
            description TEXT,
            code VARCHAR(50),
            code_type VARCHAR(50),
            payer_name VARCHAR(255),
            plan_name VARCHAR(255),
            standard_charge_gross NUMERIC(38,18),
            standard_charge_negotiated_dollar NUMERIC(38,18),
            standard_charge_min NUMERIC(38,18),
            standard_charge_max NUMERIC(38,18),
            archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            original_created_at TIMESTAMP,
            archive_reason TEXT
        );
        """
        
        cur.execute(create_table_query)
        conn.commit()
        
        logger.info("Successfully created hospital_charges_type2_archive table")
        
    except Exception as e:
        logger.error(f"Error creating hospital_charges_type2_archive table: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

if __name__ == '__main__':
    app.run(debug=True)