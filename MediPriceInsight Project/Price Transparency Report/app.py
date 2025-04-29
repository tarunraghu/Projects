from flask import Flask, jsonify, send_from_directory
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='.')

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'healthcarepoc',
    'user': 'postgres',
    'password': 'Consis10C!',
    'port': '5432'
}

def get_db_connection():
    try:
        logger.info("Attempting to connect to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        return None

@app.route('/')
def serve_index():
    return send_from_directory('.', 'index.html')

@app.route('/styles.css')
def serve_css():
    return send_from_directory('.', 'styles.css')

@app.route('/app.js')
def serve_js():
    return send_from_directory('.', 'app.js')

@app.route('/api/report')
def get_report():
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get all columns from the view directly
            logger.info("Fetching column information...")
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'hospital_dataset'
                ORDER BY ordinal_position
            """)
            columns_info = cur.fetchall()
            
            if not columns_info:
                logger.error("No columns found in the view")
                return jsonify({'error': 'View or columns not found'}), 404
            
            columns = [row['column_name'] for row in columns_info]
            logger.info(f"Found columns: {columns}")
            
            # Build the SELECT query with all columns
            select_columns = ', '.join(columns)
            query = f"""
                SELECT {select_columns}
                FROM public.hospital_dataset
                ORDER BY hospital_name, payer_name, plan_name
                LIMIT 1000
            """
            logger.info(f"Executing query: {query}")
            
            cur.execute(query)
            results = cur.fetchall()
            logger.info(f"Query returned {len(results)} rows")
            
            if len(results) > 0:
                logger.info(f"First row sample: {results[0]}")
            
            return jsonify(results)
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL Error: {str(e)}")
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({'error': f'Unexpected error: {str(e)}'}), 500
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == '__main__':
    app.run(debug=True, port=5000) 