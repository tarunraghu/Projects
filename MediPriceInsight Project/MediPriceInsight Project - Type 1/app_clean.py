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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pandas as pd
from sqlalchemy import create_engine, text
import threading
import queue
import time
from datetime import datetime, timedelta
from pyspark.sql.functions import lit, round as spark_round, col, trim, upper, count, when, split, element_at, explode, array, struct, expr, regexp_replace
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
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                .config("spark.datasource.postgresql.url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}") \
                .config("spark.datasource.postgresql.user", DB_CONFIG['user']) \
                .config("spark.datasource.postgresql.password", DB_CONFIG['password']) \
                .getOrCreate()
            
        return self._spark

    def stop_spark(self):
        if self._spark:
            self._spark.stop()
            self._spark = None

# Database configuration
DB_CONFIG = {
    "dbname": os.environ.get("DB_NAME", "healthcarepoc"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "Consis10C!"),
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": os.environ.get("DB_PORT", "5432")
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
        
        # Create a DataFrame from the address data
        df = pd.DataFrame([data])
        
        # Create SQLAlchemy engine for PostgreSQL
        engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        
        # First, delete existing record for this hospital if it exists
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM hospital_address WHERE hospital_name = :hospital_name"), 
                            {"hospital_name": data['hospital_name']})
        
        # Write DataFrame to PostgreSQL
        df.to_sql('hospital_address', engine, if_exists='append', index=False)
        
        logger.info(f"Successfully loaded address data for hospital: {data['hospital_name']}")
        
    except Exception as e:
        logger.error(f"Error loading address data: {str(e)}")
        logger.error(traceback.format_exc())
        raise 