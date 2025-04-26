from flask import Flask, jsonify, request, send_file, render_template
import os
import time
import threading
import uuid
from werkzeug.utils import secure_filename
import psycopg2
import csv
import math

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
    return psycopg2.connect(**DB_CONFIG)

def estimate_row_size(cur, table_name):
    """Estimate average row size by sampling the table"""
    try:
        cur.execute(f"""
            SELECT AVG(LENGTH(row_to_json(t)::text))
            FROM (SELECT * FROM {table_name} LIMIT 1000) t
        """)
        avg_row_size = cur.fetchone()[0] or 0
        return max(avg_row_size, 100)  # minimum 100 bytes per row
    except:
        return 1000  # fallback size estimation

def generate_dump_file(table_name, task_id):
    """Generate data dump from PostgreSQL database"""
    try:
        conn = None
        cur = None
        generated_files = []
        
        try:
            # Get database connection
            conn = get_db_connection()
            cur = conn.cursor()
            
            # Get total count for progress calculation
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cur.fetchone()[0]
            
            # For hospital_charges_cleaned, split into multiple files
            if table_name == 'hospital_charges_cleaned':
                # Estimate row size
                avg_row_size = estimate_row_size(cur, table_name)
                rows_per_file = max(int(TARGET_FILE_SIZE / avg_row_size), total_rows // 3)  # Ensure at least 3 parts
                total_files = math.ceil(total_rows / rows_per_file)
                
                # Get column names
                cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
                headers = [desc[0] for desc in cur.description]
                
                processed_rows = 0
                current_file = 1
                
                while processed_rows < total_rows:
                    # Create new file for this chunk
                    timestamp = int(time.time())
                    filename = f"{table_name}_dump_{timestamp}_part{current_file}of{total_files}.csv"
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                    generated_files.append(filename)
                    
                    rows_to_process = min(rows_per_file, total_rows - processed_rows)
                    
                    with open(filepath, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow(headers)  # Write headers
                        
                        # Process this chunk
                        cur.execute(f"""
                            SELECT * FROM {table_name} 
                            ORDER BY id
                            OFFSET {processed_rows} 
                            LIMIT {rows_to_process}
                        """)
                        
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
                
                # Get column names
                cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
                headers = [desc[0] for desc in cur.description]
                
                processed_rows = 0
                
                with open(filepath, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(headers)  # Write headers
                    
                    while processed_rows < total_rows:
                        cur.execute(f"""
                            SELECT * FROM {table_name}
                            ORDER BY id
                            OFFSET {processed_rows} 
                            LIMIT {CHUNK_SIZE}
                        """)
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
        active_tasks[task_id]['status'] = 'failed'
        active_tasks[task_id]['error'] = str(e)

@app.route('/generate-dump/<table>', methods=['POST'])
def generate_dump(table):
    if table not in ['hospital_address', 'hospital_charges_cleaned']:
        return jsonify({'success': False, 'error': 'Invalid table name'}), 400
    
    task_id = str(uuid.uuid4())
    active_tasks[task_id] = {
        'progress': 0,
        'status': 'initializing',
        'files': [],
        'error': None
    }
    
    # Start dump generation in a separate thread
    thread = threading.Thread(
        target=generate_dump_file,
        args=(table, task_id)
    )
    thread.start()
    
    return jsonify({'success': True, 'task_id': task_id})

@app.route('/dump-progress/<task_id>', methods=['GET'])
def dump_progress(task_id):
    if task_id not in active_tasks:
        return jsonify({'success': False, 'error': 'Invalid task ID'}), 404
    
    task = active_tasks[task_id]
    if task['status'] == 'failed':
        return jsonify({
            'success': False,
            'error': task['error']
        })
    
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