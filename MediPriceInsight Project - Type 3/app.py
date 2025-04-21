from flask import Flask, request, jsonify, session, render_template, redirect, url_for, send_file
from flask_cors import CORS
import logging
import psycopg2
from psycopg2 import pool
import json
from datetime import datetime
from ingestion_strategies.type1_strategy import Type1IngestionStrategy
from ingestion_strategies.type3_strategy import Type3IngestionStrategy
import os
import traceback
import csv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pandas as pd
from sqlalchemy import create_engine, text
import threading
import queue
import time
from datetime import datetime, timedelta
from pyspark.sql.functions import lit, round as spark_round, col, trim, upper, count, when, split, element_at, explode, array, struct, expr, regexp_replace, coalesce
from io import StringIO
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id, spark_partition_id
import re
import shutil
import stat
import uuid
import gzip

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
            standard_charge_gross_charge NUMERIC(20,2),
            standard_charge_negotiated_dollar NUMERIC(20,2),
            standard_charge_min NUMERIC(20,2),
            standard_charge_max NUMERIC(20,2),
            estimated_amount NUMERIC(20,2),
            standard_charge_discounted_cash NUMERIC(20,2),
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
            estimated_amount NUMERIC(20,2),
            standard_charge_discounted_cash NUMERIC(20,2),
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
        if ingestion_strategy == 'type2':
            # Type 2 specific processing
            pass
        elif ingestion_strategy == 'type3':
            strategy = Type3IngestionStrategy()
            # If df is a PySpark DataFrame, convert to Pandas first
            if hasattr(df, 'toPandas'):
                df = df.toPandas()
            # If df is a Pandas DataFrame, save it to a temporary file first
            if isinstance(df, pd.DataFrame):
                # Create a temporary file
                temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp')
                os.makedirs(temp_dir, exist_ok=True)
                temp_file = os.path.join(temp_dir, f"temp_{uuid.uuid4().hex}.csv")
                df.to_csv(temp_file, index=False)
                try:
                    return strategy.process_csv(temp_file)
                finally:
                    # Clean up the temporary file
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
            else:
                # If df is a file path, process it directly
                return strategy.process_csv(df)
        else:
            # Default to Type 1
            strategy = Type1IngestionStrategy()
            # If df is a PySpark DataFrame, convert to Pandas first
            if hasattr(df, 'toPandas'):
                df = df.toPandas()
            # If df is a Pandas DataFrame, save it to a temporary file first
            if isinstance(df, pd.DataFrame):
                # Create a temporary file
                temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'temp')
                os.makedirs(temp_dir, exist_ok=True)
                temp_file = os.path.join(temp_dir, f"temp_{uuid.uuid4().hex}.csv")
                df.to_csv(temp_file, index=False)
                try:
                    return strategy.process_csv(temp_file)
                finally:
                    # Clean up the temporary file
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
            else:
                # If df is a file path, process it directly
                return strategy.process_csv(df)
    except Exception as e:
        logger.error(f"Error processing hospital data: {str(e)}")
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
        user_name = request.form.get('userName', '').strip()
        file_type = request.form.get('fileType', '').strip().lower()  # Convert to lowercase
        ingestion_strategy = request.form.get('ingestionStrategy', '').strip().lower()  # Convert to lowercase
        
        # Validate required fields
        if not user_name:
            return jsonify({
                'success': False,
                'error': 'User name is required'
            }), 400
            
        # Validate file type - only JSON is allowed
        if file_type != 'json':
            return jsonify({
                'success': False,
                'error': 'Only JSON files are supported'
            }), 400
            
        # Validate ingestion strategy - only type3 is allowed
        if ingestion_strategy != 'type3':
            return jsonify({
                'success': False,
                'error': 'Only Type 3 ingestion strategy is supported'
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
        elif file_type == 'json' and ingestion_strategy == 'type3':
            try:
                # Initialize Type 3 strategy
                strategy = Type3IngestionStrategy()
                
                # Process JSON file
                address_df, hospital_df = strategy.read_json(file_path)
                
                # Store file information in session
                session['file_path'] = file_path
                session['user_name'] = user_name
                session['file_type'] = file_type
                session['ingestion_strategy'] = ingestion_strategy
                session['address_data'] = address_df.to_dict('records')[0]
                session['address_file'] = os.path.join(os.path.dirname(file_path), f"{os.path.splitext(os.path.basename(file_path))[0]}_address.csv")
                session['data_file'] = os.path.join(os.path.dirname(file_path), f"{os.path.splitext(os.path.basename(file_path))[0]}_data.csv")
                
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
                    'error': f'Error processing JSON file: {str(e)}'
                }), 400
        else:
            return jsonify({
                'success': False,
                'error': 'Only CSV files are supported for Type 1 and Type 2 strategies. For Type 3, both CSV and JSON files are supported.'
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

@app.route('/preview-data')
def preview_data():
    """Endpoint to get preview of the data file"""
    spark_manager = None
    try:
        # Get data file path and hospital name from session
        data_file = session.get('data_file')
        address_data = session.get('address_data')
        
        if not data_file:
            return jsonify({
                'success': False,
                'error': 'No data file found in session',
                'details': 'Please upload a file first',
                'technical_details': 'Session data_file key is missing or None'
            }), 400
            
        if not address_data or 'hospital_name' not in address_data:
            return jsonify({
                'success': False,
                'error': 'No hospital information found',
                'details': 'Please complete the address review first',
                'technical_details': 'Session address_data or hospital_name is missing'
            }), 400
            
        hospital_name = address_data['hospital_name']
            
        if not os.path.exists(data_file):
            return jsonify({
                'success': False,
                'error': 'File not found',
                'details': 'The uploaded file could not be found on the server',
                'technical_details': f'File path {data_file} does not exist'
            }), 404
            
        # Get Spark session
        spark_manager = SparkManager.get_instance()
        spark = spark_manager.get_spark()
            
        try:
            # First read the file to understand its structure
            with open(data_file, 'r', encoding='utf-8') as f:
                header_line = f.readline().strip()
                original_headers = [h.strip() for h in header_line.split(',')]
                logger.info(f"Original CSV Headers ({len(original_headers)}): {original_headers}")
            
            # Read the CSV file with permissive mode to handle extra columns
            df = spark.read \
                .option("header", "true") \
                .option("mode", "PERMISSIVE") \
                .option("encoding", "UTF-8") \
                .csv(data_file)
            
            # Get available columns
            available_columns = df.columns
            logger.info(f"Available columns in CSV: {available_columns}")
            
            # Function to safely get column reference
            def get_column(possible_names):
                for name in possible_names:
                    if name in available_columns:
                        return col(name)
                return lit(None)
            
            # Map only the columns we need, using available columns
            select_exprs = []
            
            # Description
            select_exprs.append(
                coalesce(
                    get_column(["description", "Description", "DESCRIPTION"]),
                ).alias("description")
            )
            
            # Code
            select_exprs.append(
                coalesce(
                    get_column(["code", "Code", "CODE"]),
                ).alias("code")
            )
            
            # Code Type
            select_exprs.append(
                coalesce(
                    get_column(["code_type", "Code Type", "CodeType", "CODE_TYPE"]),
                ).alias("code_type")
            )
            
            # Payer Name
            select_exprs.append(
                coalesce(
                    get_column(["payer_name", "Payer Name", "PayerName", "PAYER_NAME"]),
                ).alias("payer_name")
            )
            
            # Plan Name
            select_exprs.append(
                coalesce(
                    get_column(["plan_name", "Plan Name", "PlanName", "PLAN_NAME"]),
                ).alias("plan_name")
            )
            
            # Gross Charge
            select_exprs.append(
                coalesce(
                    get_column(["gross_charge", "Gross Charge", "GrossCharge", "GROSS_CHARGE"]),
                ).alias("standard_charge_gross_charge")
            )
            
            # Negotiated Charge
            select_exprs.append(
                coalesce(
                    get_column(["negotiated_charge", "Negotiated Rate", "NegotiatedRate", "NEGOTIATED_CHARGE"]),
                ).alias("standard_charge_negotiated_dollar")
            )
            
            # Minimum
            select_exprs.append(
                coalesce(
                    get_column(["minimum", "Minimum", "MIN", "MINIMUM"]),
                ).alias("standard_charge_min")
            )
            
            # Maximum
            select_exprs.append(
                coalesce(
                    get_column(["maximum", "Maximum", "MAX", "MAXIMUM"]),
                ).alias("standard_charge_max")
            )
            
            # Estimated Amount
            select_exprs.append(
                coalesce(
                    get_column(["estimated_amount", "Estimated Amount", "EstimatedAmount", "ESTIMATED_AMOUNT"]),
                ).alias("estimated_amount")
            )
            
            # Discounted Cash
            select_exprs.append(
                coalesce(
                    get_column(["discounted_cash", "Discounted Cash", "DiscountedCash", "DISCOUNTED_CASH"]),
                ).alias("standard_charge_discounted_cash")
            )
            
            # Create the mapped DataFrame
            mapped_df = df.select(*select_exprs)
            
            # Add hospital_name from session
            mapped_df = mapped_df.withColumn("hospital_name", lit(hospital_name))
            
            # Get preview data (first 5 rows)
            preview_df = mapped_df.limit(5)
            
            # Convert to Pandas
            pandas_df = preview_df.toPandas()
            
            # Replace any NaN values with None for JSON serialization
            preview_data = pandas_df.where(pd.notnull(pandas_df), None).to_dict('records')
            
            # Get column information
            columns = mapped_df.columns
            total_rows = mapped_df.count()
            
            # Log column information
            logger.info(f"Processed columns ({len(columns)}): {columns}")
            logger.info(f"Total rows: {total_rows}")
            
            # Log column mapping results
            logger.info("Column mapping results:")
            for col_name in columns:
                sample_values = mapped_df.select(col_name).distinct().limit(5).collect()
                logger.info(f"{col_name}: {[row[0] for row in sample_values if row[0] is not None]}")
            
            return jsonify({
                'success': True,
                'headers': columns,
                'preview_data': preview_data,
                'total_rows': total_rows,
                'original_headers': original_headers,
                'processed_columns': columns,
                'hospital_name': hospital_name
            })
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error processing data: {error_msg}")
            logger.error(traceback.format_exc())
            
            return jsonify({
                'success': False,
                'error': 'Data processing error',
                'details': error_msg,
                'technical_details': {
                    'traceback': traceback.format_exc(),
                    'original_headers': original_headers if 'original_headers' in locals() else None,
                    'available_columns': available_columns if 'available_columns' in locals() else None
                }
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
        
        # Set address_confirmed flag in session
        session['address_confirmed'] = True
        
        return jsonify({
            'success': True,
            'message': f'Successfully loaded address data for hospital: {address_data["hospital_name"]}',
            'redirect': '/review-charges'  # Add redirect URL to response
        })
        
    except Exception as e:
        logger.error(f"Error loading address data: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def process_chunks(df, chunk_size=50000, task=None):
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
            
            # Update progress if task is provided
            if task:
                progress = min(95, int((i + 1) * 100 / num_chunks))  # Cap at 95% to show final processing
                task.progress = progress
                task.message = f'Processing data: {progress}% complete ({i+1}/{num_chunks} chunks)'
            
            logger.info(f"Successfully inserted chunk {i+1}/{num_chunks} ({end_idx-start_idx+1} records)")
            
        except Exception as e:
            logger.error(f"Error inserting chunk {i+1}: {str(e)}")
            raise

def process_hospital_charges(data_file, hospital_name, task_id, user_name='system', ingestion_type='unknown', chunk_size=50000):
    """Process and load hospital charges data in chunks.
    
    Args:
        data_file (str): Path to the CSV file containing hospital charges data
        hospital_name (str): Name of the hospital
        task_id (str): Unique identifier for the processing task
        user_name (str): Name of the user initiating the process
        ingestion_type (str): Type of ingestion strategy
        chunk_size (int): Number of records to process in each chunk
    """
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
        
        # Create required tables
        create_hospital_charges_table()
        create_hospital_charges_archive_table()
        create_hospital_log_table()
        
        task.progress = 10
        task.message = 'Archiving existing records...'
        
        # Archive existing records using stored procedure
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN;")
            cur.execute("CALL archive_hospital_records(%s)", (hospital_name,))
            cur.execute("COMMIT;")
            archived_count = cur.rowcount
            log_data['archived_records'] = archived_count
            task.message = f'Archived {archived_count:,} existing records'
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error during hospital data archival: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                return_db_connection(conn)

        task.progress = 20
        task.message = 'Reading and processing data file...'

        # Create Spark session
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
            task.progress = 30
            task.message = f'Processing {total_records:,} records...'
            
            # Map columns according to the specified field mappings
            df = df.select(
                col("hospital_name"),
                col("description"),
                col("code"),
                col("code_type"),
                col("payer_name"),
                col("plan_name"),
                col("gross_charge").alias("standard_charge_gross_charge"),
                col("negotiated_charge").alias("standard_charge_negotiated_dollar"),
                col("minimum").alias("standard_charge_min"),
                col("maximum").alias("standard_charge_max"),
                col("estimated_amount"),
                col("discounted_cash").alias("standard_charge_discounted_cash")
            )
            
            # Add hospital name and timestamps
            df = df.withColumn('hospital_name', lit(hospital_name))
            current_time = datetime.now()
            df = df.withColumn('is_active', lit(True)) \
                .withColumn('created_at', lit(current_time.strftime('%Y-%m-%d %H:%M:%S')).cast('timestamp')) \
                .withColumn('updated_at', lit(current_time.strftime('%Y-%m-%d %H:%M:%S')).cast('timestamp'))
            
            task.progress = 50
            task.message = 'Removing duplicate records...'
            
            # Remove duplicates based on the specified fields
            dedup_columns = [
                'hospital_name', 'description', 'code', 'code_type',
                'payer_name', 'plan_name'
            ]
            df = df.dropDuplicates(dedup_columns)
            
            unique_records = df.count()
            if unique_records == 0:
                raise ValueError("No valid records found after processing and deduplication")
            
            log_data['unique_records'] = unique_records
            task.progress = 60
            task.message = f'Loading {unique_records:,} unique records...'
            
            # Process in chunks with progress updates
            total_chunks = (unique_records + chunk_size - 1) // chunk_size
            progress_increment = 35 / total_chunks  # Remaining 35% (60-95) divided by chunks
            
            def update_progress(chunk_num):
                progress = min(95, 60 + int(chunk_num * progress_increment))
                task.progress = progress
                task.message = f'Loading data: {progress}% complete (chunk {chunk_num + 1}/{total_chunks})'
            
            # Write data to PostgreSQL
            df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/healthcare") \
                .option("dbtable", "hospital_charges") \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            # Format elapsed time
            final_minutes = int(processing_time // 60)
            final_seconds = int(processing_time % 60)
            final_elapsed_str = f"{final_minutes}m {final_seconds}s"
            
            # Final progress update
            task.progress = 100
            task.message = (
                f'Data ingestion complete!\n'
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
                'elapsed_time': final_elapsed_str
            }
            
        except Exception as e:
            logger.error(f"Error processing hospital data: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if spark:
                spark.stop()
                
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error in process_hospital_charges: {error_message}")
        logger.error(traceback.format_exc())
        
        task.status = 'ERROR'
        task.message = f'Error: {error_message}'
        
        log_data.update({
            'end_time': datetime.now(),
            'status': 'ERROR',
            'error_message': error_message
        })
        
        # Log error details
        conn = get_db_connection()
        try:
            log_ingestion_details(conn, log_data)
        finally:
            return_db_connection(conn)
        
        raise

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
        
        # Get ingestion strategy from session
        ingestion_strategy = session.get('ingestion_strategy', 'type1')
        
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

@app.route('/confirm-address', methods=['POST'])
def confirm_address():
    try:
        # Get data from session
        data_file = session.get('data_file')
        if not data_file:
            logger.error("No data file found in session")
            return jsonify({'error': 'No data file found'}), 400
            
        # Set address_confirmed flag in session
        session['address_confirmed'] = True
        
        # Process the data file
        logger.info(f"Processing data file: {data_file}")
        result = process_file(data_file)
        
        if result.get('error'):
            logger.error(f"Error processing file: {result['error']}")
            return jsonify(result), 400
            
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in confirm_address: {str(e)}")
        return jsonify({'error': str(e)}), 500

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
    """Archive existing records for a hospital"""
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
            WHERE hospital_name = %s AND is_active = TRUE;
        """, (hospital_name,))
        
        count = cur.fetchone()[0]
        
        if count > 0:
            # Archive existing records
            cur.execute("""
                INSERT INTO hospital_charges_archive (
                    hospital_name, description, code, code_type, 
                    payer_name, plan_name, standard_charge_gross,
                    standard_charge_negotiated_dollar, standard_charge_min,
                    standard_charge_max, estimated_amount, standard_charge_discounted_cash,
                    original_created_at, archive_reason
                )
                SELECT 
                    hospital_name, description, code, code_type,
                    payer_name, plan_name, standard_charge_gross,
                    standard_charge_negotiated_dollar, standard_charge_min,
                    standard_charge_max, estimated_amount, standard_charge_discounted_cash,
                    created_at, 'New data ingestion'
                FROM hospital_charges
                WHERE hospital_name = %s AND is_active = TRUE;
            """, (hospital_name,))
            
            # Mark existing records as inactive
            cur.execute("""
                UPDATE hospital_charges 
                SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
                WHERE hospital_name = %s AND is_active = TRUE;
            """, (hospital_name,))
            
            # Commit transaction
            conn.commit()
            
            logger.info(f"Successfully archived {count} records for hospital: {hospital_name}")
            return count
        else:
            # Commit transaction even if no records to archive
            conn.commit()
            logger.info(f"No active records found to archive for hospital: {hospital_name}")
            return 0
            
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error archiving hospital records: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

@app.route('/dump-data')
def dump_data_page():
    """Route to render the simplified data dump page"""
    return render_template('dump_data.html')

@app.route('/generate-dump/<table>', methods=['POST'])
def generate_table_dump(table):
    """Generate complete dump for the specified table, split into multiple files if needed"""
    task_id = str(uuid.uuid4())
    try:
        # Validate table name
        if table not in ['hospital_address', 'hospital_charges']:
            return jsonify({
                'success': False,
                'error': 'Invalid table name'
            }), 400
        
        # Initialize progress tracking
        dump_progress[task_id] = 0
        dump_status[task_id] = 'initializing'
        
        # Create downloads directory
        downloads_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads')
        os.makedirs(downloads_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"{table}_dump_{timestamp}"
        filename = f"{base_filename}.csv"
        filepath = os.path.join(downloads_dir, filename)
            
        # Update progress
        dump_progress[task_id] = 10
        dump_status[task_id] = 'querying_database'
        
        # Build query
        query = f"SELECT * FROM {table}"
        if table == 'hospital_charges':
            query += " WHERE is_active = TRUE"
        query += " ORDER BY id"
        
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            
            # Get column names once - these will be used as headers for all files
            query = f"SELECT * FROM {table} LIMIT 0"  # Get column names without fetching data
            cur.execute(query)
            headers = [desc[0] for desc in cur.description]
            logger.info(f"Headers for {table}: {headers}")
            
            # Get total count for progress calculation
            count_query = f"SELECT COUNT(*) FROM {table}"
            if table == 'hospital_charges':
                count_query += " WHERE is_active = TRUE"
            cur.execute(count_query)
            total_rows = cur.fetchone()[0]
            
            # Build main query
            query = f"SELECT * FROM {table}"
            if table == 'hospital_charges':
                query += " WHERE is_active = TRUE"
            query += " ORDER BY id"
            
            # Execute main query
            cur.execute(query)
            
            # Initialize file tracking
            generated_files = []
            total_rows_written = 0
            current_file_number = 1
            rows_per_file = 500000
            num_files = (total_rows + rows_per_file - 1) // rows_per_file
            
            logger.info(f"Starting dump of {total_rows:,} rows, will create {num_files} files")
            
            # Write data in chunks across multiple files
            while True:
                # Create new file
                if num_files > 1:
                    filename = f"{base_filename}_part{current_file_number}.csv"
                else:
                    filename = f"{base_filename}.csv"
                    
                filepath = os.path.join(downloads_dir, filename)
                rows_in_current_file = 0
                
                logger.info(f"Creating file {current_file_number}/{num_files}: {filename}")
                
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)  # Write the same headers to each file
                    
                    # Write rows for this file
                    while rows_in_current_file < rows_per_file:
                        rows = cur.fetchmany(10000)  # Fetch in smaller chunks
                        if not rows:
                            break
                            
                        writer.writerows(rows)
                        rows_written = len(rows)
                        rows_in_current_file += rows_written
                        total_rows_written += rows_written
                        
                        # Update progress
                        if total_rows > 0:
                            progress = min(90, 10 + (80 * total_rows_written / total_rows))
                            dump_progress[task_id] = progress
                            dump_status[task_id] = (
                                f'Writing file {current_file_number} of {num_files} '
                                f'({total_rows_written:,} of {total_rows:,} total rows)'
                            )
                
                # Verify file was created with correct headers
                with open(filepath, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if first_line != ','.join(headers):
                        raise ValueError(f"Header verification failed for file {filename}")
                
                # Record file information
                file_size = os.path.getsize(filepath)
                generated_files.append({
                    'filename': filename,
                    'size': file_size,
                    'rows': rows_in_current_file + 1,  # +1 for header
                    'path': filepath,
                    'part': current_file_number if num_files > 1 else None
                })
                
                logger.info(f"Completed file {filename} with {rows_in_current_file:,} rows")
                
                # Check if we've processed all rows
                if not rows:
                    break
                    
                current_file_number += 1
            
            # Store file information in session
            session['download_files'] = generated_files
            
            # Update final progress
            dump_progress[task_id] = 100
            dump_status[task_id] = 'completed'
            
            return jsonify({
                'success': True,
                'task_id': task_id,
                'message': (
                    f'Successfully generated dump with {total_rows_written:,} rows '
                    f'across {len(generated_files)} file(s)'
                ),
                'headers': headers  # Include headers in response for verification
            })
            
        finally:
            if cur:
                cur.close()
            if conn:
                return_db_connection(conn)
                
    except Exception as e:
        error_msg = f"Error generating dump: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        dump_status[task_id] = 'failed'
        return jsonify({
            'success': False,
            'error': error_msg
        }), 500

@app.route('/get-hospitals')
def get_hospitals():
    """Get list of all hospitals from the database"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT DISTINCT hospital_name 
            FROM hospital_address 
            ORDER BY hospital_name
        """)
        
        hospitals = [row[0] for row in cur.fetchall()]
        
        return jsonify({
            'success': True,
            'hospitals': hospitals
        })
        
    except Exception as e:
        logger.error(f"Error getting hospitals: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

def configure_spark_session():
    """Configure and create a Spark session with appropriate logging"""
    try:
        # Create builder with basic configs
        builder = SparkSession.builder \
            .appName("Data Dump Generation") \
            .config("spark.jars", os.path.abspath("postgresql-42.7.2.jar")) \
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g")
        
        # Create and configure the session
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    except Exception as e:
        logger.error(f"Failed to configure Spark session: {str(e)}")
        raise

@app.route('/dump-progress/<task_id>')
def get_dump_progress(task_id):
    """Get the progress of a data dump operation"""
    try:
        if task_id not in dump_progress:
            return jsonify({
                'success': False,
                'error': 'Invalid task ID'
            }), 404
            
        return jsonify({
            'success': True,
            'progress': dump_progress[task_id],
            'status': dump_status[task_id]
        })
        
    except Exception as e:
        logger.error(f"Error getting dump progress: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/download/<filename>')
def download_file(filename):
    """Download the generated CSV file"""
    try:
        filepath = session.get('download_file')
        if not filepath:
            logger.error("No file path found in session")
            return jsonify({
                'success': False,
                'error': 'No file path found in session'
            }), 404
            
        if not os.path.exists(filepath):
            logger.error(f"File not found at path: {filepath}")
            return jsonify({
                'success': False,
                'error': 'File not found'
            }), 404
            
        logger.info(f"Serving file: {filepath}")
        
        return send_file(
            filepath,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
        
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Add cleanup function at the start of the file, after imports
def cleanup_old_temp_dirs():
    """Clean up old temporary directories on startup"""
    base_temp_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'spark_temp')
    cleanup_file = os.path.join(base_temp_dir, 'cleanup.txt')
    
    if os.path.exists(cleanup_file):
        try:
            with open(cleanup_file, 'r') as f:
                dirs_to_clean = f.readlines()
            
            # Remove the cleanup file first
            os.remove(cleanup_file)
            
            # Try to clean up each directory
            for dir_path in dirs_to_clean:
                dir_path = dir_path.strip()
                if os.path.exists(dir_path):
                    try:
                        shutil.rmtree(dir_path, ignore_errors=True)
                    except Exception as e:
                        logger.warning(f"Failed to clean up old temp directory {dir_path}: {str(e)}")
        except Exception as e:
            logger.warning(f"Error during cleanup of old temp directories: {str(e)}")

# At the top of the file, add this function
def cleanup_spark_temp_dirs():
    """Clean up Spark temporary directories"""
    temp_dir = os.path.join(os.environ.get('TEMP', os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'Temp')))
    try:
        for item in os.listdir(temp_dir):
            if item.startswith('spark-'):
                spark_dir = os.path.join(temp_dir, item)
                try:
                    # Use a more robust cleanup approach for Windows
                    def on_rm_error(func, path, exc_info):
                        # Try changing permissions and try again
                        os.chmod(path, stat.S_IWRITE)
                        func(path)
                    
                    shutil.rmtree(spark_dir, onerror=on_rm_error)
                    logger.info(f"Successfully cleaned up Spark temp directory: {spark_dir}")
                except Exception as e:
                    logger.warning(f"Failed to clean up Spark temp directory {spark_dir}: {str(e)}")
    except Exception as e:
        logger.warning(f"Error during Spark temp directory cleanup: {str(e)}")

# Add cleanup call at application startup
if __name__ == '__main__':
    cleanup_old_temp_dirs()
    app.run(debug=True)

# Add this new route to get hospitals for the dropdown
@app.route('/api/hospitals-list')
def get_hospitals_list():
    """Get list of all hospitals for dropdown"""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Query to get hospitals with their record counts
        query = """
        SELECT 
            ha.hospital_name,
            COUNT(DISTINCT CASE WHEN hc.is_active = TRUE THEN hc.id END) as active_records
        FROM 
            hospital_address ha
            LEFT JOIN hospital_charges hc ON ha.hospital_name = hc.hospital_name
        GROUP BY 
            ha.hospital_name
        ORDER BY 
            ha.hospital_name;
        """
        
        cur.execute(query)
        hospitals = [{"name": row[0], "record_count": row[1]} for row in cur.fetchall()]
        
        return jsonify({
            'success': True,
            'hospitals': hospitals
        })
        
    except Exception as e:
        logger.error(f"Error fetching hospitals list: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
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
        
        count = cur.fetchone()[0]
        
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