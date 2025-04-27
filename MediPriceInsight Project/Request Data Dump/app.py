from flask import Flask, jsonify, request, send_file, render_template
import os
import time
import threading
import uuid
from werkzeug.utils import secure_filename
import psycopg2
import csv
import math
import logging
import sys

# Configure logging to show in console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'downloads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
TARGET_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB in bytes
CHUNK_SIZE = 5000  # Increased chunk size for better performance

# Database configuration
DB_CONFIG = {
    'dbname': 'healthcarepoc',
    'user': 'postgres',
    'password': 'Consis10C!',
    'host': 'localhost',
    'port': '5432'
}

# Ensure the downloads directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Store active tasks
active_tasks = {}

@app.route('/')
def index():
    return send_file('dump_data.html')

def get_db_connection():
    """Create and return a database connection"""
    start_time = time.time()
    try:
        logger.info("Attempting to connect to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info(f"Database connection established in {time.time() - start_time:.2f} seconds")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise

def estimate_row_size(cur, table_name):
    """Estimate average row size by sampling the table"""
    start_time = time.time()
    try:
        cur.execute(f"""
            SELECT AVG(LENGTH(row_to_json(t)::text))
            FROM (SELECT * FROM {table_name} LIMIT 1000) t
        """)
        avg_row_size = cur.fetchone()[0] or 0
        result = max(avg_row_size, 100)  # minimum 100 bytes per row
        logger.info(f"Row size estimation completed in {time.time() - start_time:.2f} seconds")
        return result
    except Exception as e:
        logger.error(f"Failed to estimate row size: {str(e)}")
        return 1000  # fallback size estimation

def generate_dump_file(table_name, task_id):
    """Generate data dump from PostgreSQL database"""
    try:
        conn = None
        cur = None
        generated_files = []
        
        # Map hospital_charges_cleaned to public.hospital_dataset
        actual_table = 'public.hospital_dataset' if table_name == 'hospital_charges_cleaned' else table_name
        
        try:
            # Get database connection
            logger.info(f"Starting dump generation for table: {actual_table}")
            active_tasks[task_id]['status'] = 'Connecting to database...'
            
            conn = get_db_connection()
            cur = conn.cursor()
            
            # Get column information
            logger.info("Retrieving column information...")
            active_tasks[task_id]['status'] = 'Retrieving column information...'
            
            cur.execute(f"SELECT * FROM {actual_table} LIMIT 0")
            headers = [desc[0] for desc in cur.description]
            logger.info(f"Available columns: {headers}")
            
            # Get total count for progress calculation
            logger.info("Counting total rows...")
            active_tasks[task_id]['status'] = 'Counting rows...'
            
            start_time = time.time()
            cur.execute(f"SELECT COUNT(*) FROM {actual_table}")
            total_rows = cur.fetchone()[0]
            logger.info(f"Total rows count: {total_rows} (took {time.time() - start_time:.2f} seconds)")
            
            # For hospital_charges_cleaned, split into multiple files
            if table_name == 'hospital_charges_cleaned':
                # Estimate row size
                logger.info("Estimating row size...")
                active_tasks[task_id]['status'] = 'Estimating row size...'
                
                avg_row_size = estimate_row_size(cur, actual_table)
                rows_per_file = max(int(TARGET_FILE_SIZE / avg_row_size), total_rows // 3)  # Ensure at least 3 parts
                total_files = math.ceil(total_rows / rows_per_file)
                logger.info(f"Will split into {total_files} files, approximately {rows_per_file} rows per file")
                
                processed_rows = 0
                current_file = 1
                
                while processed_rows < total_rows:
                    # Create new file for this chunk
                    timestamp = int(time.time())
                    filename = f"{table_name}_dump_{timestamp}_part{current_file}of{total_files}.csv"
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                    generated_files.append(filename)
                    
                    rows_to_process = min(rows_per_file, total_rows - processed_rows)
                    logger.info(f"Processing file {current_file} of {total_files} ({rows_to_process} rows)")
                    active_tasks[task_id]['status'] = f'Processing file {current_file} of {total_files}...'
                    
                    with open(filepath, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow(headers)  # Write headers
                        
                        # Process this chunk
                        start_time = time.time()
                        query = f"""
                            SELECT * FROM {actual_table}
                            ORDER BY id
                            OFFSET {processed_rows} 
                            LIMIT {rows_to_process}
                        """
                        logger.info(f"Executing query: {query}")
                        cur.execute(query)
                        logger.info(f"Query executed in {time.time() - start_time:.2f} seconds")
                        
                        while True:
                            rows = cur.fetchmany(CHUNK_SIZE)
                            if not rows:
                                break
                                
                            for row in rows:
                                writer.writerow([str(val) if val is not None else '' for val in row])
                                processed_rows += 1
                                
                                # Update progress less frequently for better performance
                                if processed_rows % 1000 == 0:
                                    progress = int((processed_rows / total_rows) * 100)
                                    active_tasks[task_id]['progress'] = progress
                                    active_tasks[task_id]['status'] = f'Processing part {current_file} of {total_files}... {progress}%'
                    
                    current_file += 1
                
                active_tasks[task_id]['status'] = f'completed (Split into {total_files} files)'
                active_tasks[task_id]['files'] = generated_files
                
            else:
                # Original single-file processing for other tables
                filename = f"{table_name}_dump_{int(time.time())}.csv"
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                generated_files.append(filename)
                
                processed_rows = 0
                
                with open(filepath, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(headers)  # Write headers
                    
                    while processed_rows < total_rows:
                        query = f"""
                            SELECT * FROM {actual_table}
                            ORDER BY id
                            OFFSET {processed_rows} 
                            LIMIT {CHUNK_SIZE}
                        """
                        logger.info(f"Executing query: {query}")
                        cur.execute(query)
                        rows = cur.fetchall()
                        
                        for row in rows:
                            writer.writerow([str(val) if val is not None else '' for val in row])
                            processed_rows += 1
                            
                            # Update progress less frequently
                            if processed_rows % 1000 == 0:
                                progress = int((processed_rows / total_rows) * 100)
                                active_tasks[task_id]['progress'] = progress
                                active_tasks[task_id]['status'] = f'Processing... {progress}%'
                        
                        if not rows:  # No more rows to process
                            break
                
                active_tasks[task_id]['status'] = 'completed'
                active_tasks[task_id]['files'] = generated_files
            
        finally:
            # Clean up resources
            if cur:
                cur.close()
            if conn:
                conn.close()
                
    except Exception as e:
        logger.error(f"Error during dump generation: {str(e)}")
        active_tasks[task_id]['status'] = 'failed'
        active_tasks[task_id]['error'] = str(e)

@app.route('/generate-dump/<table>', methods=['POST'])
def generate_dump(table):
    logger.info(f"Received request to generate dump for table: {table}")
    if table not in ['hospital_address', 'hospital_charges_cleaned']:
        logger.error(f"Invalid table name: {table}")
        return jsonify({'success': False, 'error': 'Invalid table name'}), 400
    
    task_id = str(uuid.uuid4())
    logger.info(f"Created new task with ID: {task_id}")
    
    active_tasks[task_id] = {
        'progress': 0,
        'status': 'initializing',
        'files': [],
        'error': None,
        'start_time': time.time()
    }
    
    # Start dump generation in a separate thread
    thread = threading.Thread(
        target=generate_dump_file,
        args=(table, task_id)
    )
    thread.start()
    
    logger.info(f"Started dump generation thread for task {task_id}")
    return jsonify({'success': True, 'task_id': task_id})

@app.route('/dump-progress/<task_id>', methods=['GET'])
def dump_progress(task_id):
    if task_id not in active_tasks:
        logger.warning(f"Invalid task ID requested: {task_id}")
        return jsonify({'success': False, 'error': 'Invalid task ID'}), 404
    
    task = active_tasks[task_id]
    if task['status'] == 'failed':
        logger.error(f"Task {task_id} failed: {task['error']}")
        return jsonify({
            'success': False,
            'error': task['error']
        })
    
    # Log progress if it's changed
    if task['progress'] > 0:
        elapsed_time = time.time() - task['start_time']
        logger.info(f"Task {task_id} progress: {task['progress']}% (elapsed: {elapsed_time:.1f}s)")
    
    return jsonify({
        'success': True,
        'progress': task['progress'],
        'status': task['status'],
        'files': task['files'] if task['status'].startswith('completed') else []
    })

@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    try:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(filename))
        if not os.path.exists(filepath):
            return jsonify({'success': False, 'error': 'File not found'}), 404
        
        return send_file(
            filepath,
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5004, debug=True) 