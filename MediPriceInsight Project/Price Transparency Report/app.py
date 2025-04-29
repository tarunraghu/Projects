from flask import Flask, jsonify, send_from_directory, request
from flask_caching import Cache
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import logging
from datetime import datetime
import time

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='.')

# Configure Flask-Caching
cache = Cache(app, config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 300  # 5 minutes
})

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'healthcarepoc')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Consis10C!')
DB_PORT = os.getenv('DB_PORT', '5432')

# Performance monitoring
def log_performance(func_name, start_time, row_count=None):
    duration = time.time() - start_time
    if row_count:
        logger.info(f"Performance: {func_name} - {duration:.2f}s - {row_count} rows")
    else:
        logger.info(f"Performance: {func_name} - {duration:.2f}s")

def get_db_connection():
    logger.info("Attempting to connect to database...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            cursor_factory=RealDictCursor
        )
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

@app.route('/')
def serve_index():
    return send_from_directory('.', 'index.html')

@app.route('/styles.css')
def serve_css():
    return send_from_directory('.', 'styles.css')

@app.route('/app.js')
def serve_js():
    return send_from_directory('.', 'app.js')

def get_distinct_values(cur, column):
    """Helper function to get distinct values for a column"""
    query = f"""
        SELECT DISTINCT {column}
        FROM public.hospital_dataset
        WHERE {column} IS NOT NULL
        ORDER BY {column}
    """
    cur.execute(query)
    return [row[column] for row in cur.fetchall()]

@app.route('/api/filters')
@cache.cached(timeout=300)  # Cache for 5 minutes
def get_filters():
    start_time = time.time()
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get distinct values for each filter separately
            filter_values = {
                'code': get_distinct_values(cur, 'code'),
                'city': get_distinct_values(cur, 'city'),
                'region': get_distinct_values(cur, 'region'),
                'payer_name': get_distinct_values(cur, 'payer_name'),
                'plan_name': get_distinct_values(cur, 'plan_name')
            }
            
            duration = time.time() - start_time
            logger.info(f"Filters fetched in {duration:.2f}s")
            logger.info(f"Filter counts: " + 
                       f"codes={len(filter_values['code'])}, " +
                       f"cities={len(filter_values['city'])}, " +
                       f"regions={len(filter_values['region'])}, " +
                       f"payers={len(filter_values['payer_name'])}, " +
                       f"plans={len(filter_values['plan_name'])}")
            
            return jsonify(filter_values)
    except Exception as e:
        logger.error(f"Error fetching filter values: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/report')
def get_report():
    start_time = time.time()
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 100))
        offset = (page - 1) * per_page

        # Get filter parameters
        code_search = request.args.get('code')  # This will now be used for both code and description search
        city = request.args.get('city')
        region = request.args.get('region')
        payer_name = request.args.get('payer_name')
        plan_name = request.args.get('plan_name')

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Build WHERE clause
            conditions = []
            params = []
            
            if code_search:
                conditions.append("(code ILIKE %s OR description ILIKE %s)")
                search_pattern = f"%{code_search}%"
                params.extend([search_pattern, search_pattern])
            if city:
                conditions.append("city = %s")
                params.append(city)
            if region:
                conditions.append("region = %s")
                params.append(region)
            if payer_name:
                conditions.append("payer_name = %s")
                params.append(payer_name)
            if plan_name:
                conditions.append("plan_name = %s")
                params.append(plan_name)

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            # Get total count
            count_query = f"""
                SELECT COUNT(*)
                FROM public.hospital_dataset
                WHERE {where_clause}
            """
            cur.execute(count_query, params)
            total_count = cur.fetchone()['count']

            # Get paginated data
            if total_count > 0:
                data_query = f"""
                    SELECT 
                        id, hospital_name, code, description,
                        hospital_address, city, region,
                        payer_name, plan_name,
                        standard_charge_min, standard_charge_max,
                        standard_charge_gross, standard_charge_negotiated_dollar
                    FROM public.hospital_dataset
                    WHERE {where_clause}
                    ORDER BY hospital_name, payer_name, plan_name
                    LIMIT %s OFFSET %s
                """
                params.extend([per_page, offset])
                cur.execute(data_query, params)
                results = cur.fetchall()
            else:
                results = []

            duration = time.time() - start_time
            logger.info(f"Data fetched in {duration:.2f}s - {len(results)} rows")
            
            return jsonify({
                'data': results,
                'total': total_count,
                'page': page,
                'per_page': per_page,
                'total_pages': (total_count + per_page - 1) // per_page
            })
    except Exception as e:
        logger.error(f"Error fetching report data: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/report/codes')
@cache.cached(timeout=3600)  # Cache for 1 hour since codes rarely change
def get_codes():
    try:
        start_time = time.time()
        conn = get_db_connection()
        cur = conn.cursor()

        # Efficient query to get distinct codes and descriptions
        query = """
        SELECT DISTINCT code, description 
        FROM public.hospital_dataset 
        WHERE code IS NOT NULL 
        ORDER BY code;
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        # Convert results to list of dicts
        data = [dict(row) for row in results]
        
        cur.close()
        conn.close()
        
        execution_time = time.time() - start_time
        logger.info(f"Codes fetched in {execution_time:.2f}s - {len(data)} unique codes")
        
        return jsonify({
            'status': 'success',
            'data': data,
            'count': len(data)
        })
        
    except Exception as e:
        logger.error(f"Error fetching codes: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'Failed to fetch codes',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000) 