from flask import Flask, jsonify, send_from_directory, request
from flask_caching import Cache
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import logging
from datetime import datetime
import time
from flask_cors import CORS
import requests
import json

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='.')
CORS(app)

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

OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"

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
        # Get filter parameters
        code = request.args.get('code')
        fields = request.args.get('fields')
        cities = request.args.getlist('city')
        regions = request.args.getlist('region')
        hospital_names = request.args.getlist('hospital_name')
        payer_names = request.args.getlist('payer_name')
        plan_names = request.args.getlist('plan_name')

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Build WHERE clause
            conditions = []
            params = []

            if code:
                conditions.append("code = %s")
                params.append(code)
            if cities:
                placeholders = ','.join(['%s'] * len(cities))
                conditions.append(f"city IN ({placeholders})")
                params.extend(cities)
            if regions:
                placeholders = ','.join(['%s'] * len(regions))
                conditions.append(f"region IN ({placeholders})")
                params.extend(regions)
            if hospital_names:
                placeholders = ','.join(['%s'] * len(hospital_names))
                conditions.append(f"hospital_name IN ({placeholders})")
                params.extend(hospital_names)
            if payer_names:
                placeholders = ','.join(['%s'] * len(payer_names))
                conditions.append(f"payer_name IN ({placeholders})")
                params.extend(payer_names)
            if plan_names:
                placeholders = ','.join(['%s'] * len(plan_names))
                conditions.append(f"plan_name IN ({placeholders})")
                params.extend(plan_names)

            where_clause = " AND ".join(conditions) if conditions else "1=1"

            # For code queries, no pagination
            if code:
                if fields:
                    select_fields = fields
                else:
                    select_fields = """
                        id, hospital_name, code, description,
                        hospital_address, city, region,
                        payer_name, plan_name,
                        standard_charge_min, standard_charge_max,
                        standard_charge_gross, standard_charge_negotiated_dollar
                    """
                data_query = f"""
                    SELECT {select_fields}
                    FROM public.hospital_dataset
                    WHERE {where_clause}
                    ORDER BY hospital_name, payer_name, plan_name
                """
                cur.execute(data_query, params)
                results = cur.fetchall()
                total_count = len(results)
                duration = time.time() - start_time
                logger.info(f"Data fetched in {duration:.2f}s - {total_count} rows")
                return jsonify({
                    'status': 'success',
                    'data': results,
                    'total': total_count
                })
            else:
                # For non-code queries, keep pagination
                page = int(request.args.get('page', 1))
                per_page = int(request.args.get('per_page', 100))
                offset = (page - 1) * per_page

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
                    'status': 'success',
                    'data': results,
                    'total': total_count,
                    'page': page,
                    'per_page': per_page,
                    'total_pages': (total_count + per_page - 1) // per_page
                })

    except Exception as e:
        logger.error(f"Error fetching report data: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'Failed to fetch report data',
            'error': str(e)
        }), 500
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

def query_llm(prompt, model="llama2"):
    try:
        response = requests.post(OLLAMA_ENDPOINT, 
            json={
                "model": model,
                "prompt": prompt,
                "stream": False
            })
        return response.json()["response"]
    except Exception as e:
        print(f"Error querying LLM: {e}")
        return str(e)

def execute_safe_query(query, params=None):
    """Execute a SQL query safely and return results"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            results = cur.fetchall()
            return results
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

@app.route('/analyze', methods=['POST'])
def analyze_prices():
    try:
        data = request.json
        query_type = data.get('type', 'general')
        user_query = data.get('query', '')

        # First, ask Llama 2 to generate a SQL query
        sql_prompt = f"""You are a healthcare data analyst. Generate a SQL query to answer this question:

        Question: {user_query}

        The data is in a view called hospital_dataset with these columns:
        - id: unique identifier
        - hospital_name: name of the hospital
        - code: procedure code
        - description: procedure description
        - hospital_address: full address
        - city: city location
        - region: region/state
        - payer_name: insurance provider
        - plan_name: specific insurance plan
        - standard_charge_min: minimum charge
        - standard_charge_max: maximum charge
        - standard_charge_gross: gross charge
        - standard_charge_negotiated_dollar: negotiated rate

        Requirements:
        1. Return ONLY the SQL query, no explanations
        2. Use only SELECT statements (no INSERT, UPDATE, DELETE)
        3. Include proper aggregations (AVG, COUNT, etc.) when needed
        4. Limit results to 100 rows for performance
        5. Format numbers using ROUND() for readability
        6. Order results meaningfully
        7. Use clear column aliases

        SQL Query:"""

        # Get the SQL query from Llama 2
        sql_query = query_llm(sql_prompt).strip()
        
        # Execute the query and get results
        try:
            results = execute_safe_query(sql_query)
            
            # Now ask Llama 2 to analyze the results
            analysis_prompt = f"""Analyze these healthcare pricing results:

            Question: {user_query}

            SQL Query Used:
            {sql_query}

            Query Results (first 5 rows shown):
            {json.dumps(results[:5], indent=2)}

            Total Results: {len(results)} rows

            Please provide:
            1. A clear summary of the findings
            2. Key insights from the data
            3. Notable patterns or trends
            4. Recommendations based on the analysis

            Keep the response conversational and easy to understand."""

            analysis = query_llm(analysis_prompt)

            return jsonify({
                "status": "success",
                "query": sql_query,
                "results": results,
                "analysis": analysis,
                "query_type": query_type
            })

        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            # If query fails, ask Llama 2 for help debugging
            debug_prompt = f"""The following SQL query failed:

            {sql_query}

            Error: {str(e)}

            Please suggest a corrected version of the query that will work with our schema.
            Return ONLY the corrected SQL query, no explanations."""

            corrected_query = query_llm(debug_prompt).strip()
            
            try:
                # Try the corrected query
                results = execute_safe_query(corrected_query)
                
                return jsonify({
                    "status": "success",
                    "query": corrected_query,
                    "results": results,
                    "analysis": f"I had to modify the query to work with our database. Here are the results based on the corrected query.",
                    "query_type": query_type
                })
            except Exception as e2:
                logger.error(f"Corrected query also failed: {str(e2)}")
                return jsonify({
                    "status": "error",
                    "message": "Could not generate a working query for your question. Please try rephrasing it.",
                    "error": str(e2)
                }), 500

    except Exception as e:
        logger.error(f"Error in analyze_prices: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "An error occurred while analyzing the data",
            "error": str(e)
        }), 500

@app.route('/summarize', methods=['POST'])
def summarize_data():
    data = request.json
    hospital_data = data.get('data', [])

    prompt = f"""
    Create a summary of this healthcare price transparency data:
    {json.dumps(hospital_data, indent=2)}

    Include:
    1. Price range overview
    2. Key findings about price variations
    3. Notable patterns in insurance coverage
    4. Potential cost-saving opportunities

    Format the response in clear, bullet-point sections.
    """

    summary = query_llm(prompt)
    return jsonify({"summary": summary})

@app.route('/api/filters/regions')
def get_regions():
    try:
        print("Connecting to DB for regions...")
        conn = get_db_connection()
        print("Connected. Creating cursor...")
        with conn.cursor() as cur:
            print("Executing query...")
            cur.execute("SELECT DISTINCT region FROM public.hospital_dataset WHERE region IS NOT NULL ORDER BY region;")
            regions = [row['region'] for row in cur.fetchall()]
            print("Regions fetched:", regions)
        return jsonify(regions)
    except Exception as e:
        import traceback
        print("Error in /api/filters/regions:", traceback.format_exc())
        print("Exception message:", str(e))
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/filters/cities')
def get_cities():
    try:
        regions = request.args.getlist('regions')
        print("Requested regions for cities:", regions)
        conn = get_db_connection()
        with conn.cursor() as cur:
            if regions:
                placeholders = ','.join(['%s'] * len(regions))
                query = f"""
                    SELECT DISTINCT city FROM public.hospital_dataset
                    WHERE region IN ({placeholders}) AND city IS NOT NULL
                    ORDER BY city;
                """
                cur.execute(query, regions)
            else:
                query = """
                    SELECT DISTINCT city FROM public.hospital_dataset
                    WHERE city IS NOT NULL
                    ORDER BY city;
                """
                cur.execute(query)
            cities = [row['city'] for row in cur.fetchall()]
            print("Cities fetched:", cities)
        return jsonify(cities)
    except Exception as e:
        import traceback
        print("Error in /api/filters/cities:", traceback.format_exc())
        print("Exception message:", str(e))
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/filters/codes')
def get_codes_for_region_city():
    try:
        regions = request.args.getlist('regions')
        cities = request.args.getlist('cities')
        print("Requested codes for regions:", regions, "and cities:", cities)
        conn = get_db_connection()
        with conn.cursor() as cur:
            conditions = []
            params = []
            if regions:
                placeholders = ','.join(['%s'] * len(regions))
                conditions.append(f"region IN ({placeholders})")
                params.extend(regions)
            if cities:
                placeholders = ','.join(['%s'] * len(cities))
                conditions.append(f"city IN ({placeholders})")
                params.extend(cities)
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = f"""
                SELECT DISTINCT code, description
                FROM public.hospital_dataset
                WHERE {where_clause} AND code IS NOT NULL
                ORDER BY code;
            """
            cur.execute(query, params)
            codes = [{'code': row['code'], 'description': row['description']} for row in cur.fetchall()]
            print("Codes fetched:", codes)
        return jsonify(codes)
    except Exception as e:
        import traceback
        print("Error in /api/filters/codes:", traceback.format_exc())
        print("Exception message:", str(e))
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/filters/hospitals')
def get_hospitals():
    try:
        regions = request.args.getlist('regions')
        cities = request.args.getlist('cities')
        code = request.args.get('code')
        print("Requested hospitals for regions:", regions, "cities:", cities, "code:", code)
        conn = get_db_connection()
        with conn.cursor() as cur:
            conditions = []
            params = []
            if regions:
                placeholders = ','.join(['%s'] * len(regions))
                conditions.append(f"region IN ({placeholders})")
                params.extend(regions)
            if cities:
                placeholders = ','.join(['%s'] * len(cities))
                conditions.append(f"city IN ({placeholders})")
                params.extend(cities)
            if code:
                conditions.append("code = %s")
                params.append(code)
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = f"""
                SELECT DISTINCT hospital_name
                FROM public.hospital_dataset
                WHERE {where_clause} AND hospital_name IS NOT NULL
                ORDER BY hospital_name;
            """
            cur.execute(query, params)
            hospitals = [row['hospital_name'] for row in cur.fetchall()]
            print("Hospitals fetched:", hospitals)
        return jsonify(hospitals)
    except Exception as e:
        import traceback
        print("Error in /api/filters/hospitals:", traceback.format_exc())
        print("Exception message:", str(e))
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/filters/payers')
def get_payers():
    try:
        regions = request.args.getlist('regions')
        cities = request.args.getlist('cities')
        code = request.args.get('code')
        hospitals = request.args.getlist('hospital_name')
        print("Requested payers for regions:", regions, "cities:", cities, "code:", code, "hospitals:", hospitals)
        conn = get_db_connection()
        with conn.cursor() as cur:
            conditions = []
            params = []
            if regions:
                placeholders = ','.join(['%s'] * len(regions))
                conditions.append(f"region IN ({placeholders})")
                params.extend(regions)
            if cities:
                placeholders = ','.join(['%s'] * len(cities))
                conditions.append(f"city IN ({placeholders})")
                params.extend(cities)
            if code:
                conditions.append("code = %s")
                params.append(code)
            if hospitals:
                placeholders = ','.join(['%s'] * len(hospitals))
                conditions.append(f"hospital_name IN ({placeholders})")
                params.extend(hospitals)
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = f'''
                SELECT payer_name, COUNT(*) as count
                FROM public.hospital_dataset
                WHERE {where_clause} AND payer_name IS NOT NULL
                GROUP BY payer_name
                ORDER BY payer_name;
            '''
            print("SQL:", query)
            print("Params:", params)
            cur.execute(query, params)
            payers = [{"name": row['payer_name'], "count": row['count']} for row in cur.fetchall()]
            print("Payers fetched:", payers)
        return jsonify(payers)
    except Exception as e:
        import traceback
        print("Error in /api/filters/payers:", traceback.format_exc())
        print("Exception message:", str(e))
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/filters/plans')
def get_plans():
    try:
        regions = request.args.getlist('regions')
        cities = request.args.getlist('cities')
        code = request.args.get('code')
        hospitals = request.args.getlist('hospital_name')
        payers = request.args.getlist('payer_name')
        print("Requested plans for regions:", regions, "cities:", cities, "code:", code, "hospitals:", hospitals, "payers:", payers)
        conn = get_db_connection()
        with conn.cursor() as cur:
            conditions = []
            params = []
            if regions:
                placeholders = ','.join(['%s'] * len(regions))
                conditions.append(f"region IN ({placeholders})")
                params.extend(regions)
            if cities:
                placeholders = ','.join(['%s'] * len(cities))
                conditions.append(f"city IN ({placeholders})")
                params.extend(cities)
            if code:
                conditions.append("code = %s")
                params.append(code)
            if hospitals:
                placeholders = ','.join(['%s'] * len(hospitals))
                conditions.append(f"hospital_name IN ({placeholders})")
                params.extend(hospitals)
            if payers:
                placeholders = ','.join(['%s'] * len(payers))
                conditions.append(f"payer_name IN ({placeholders})")
                params.extend(payers)
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            query = f'''
                SELECT plan_name, COUNT(*) as count
                FROM public.hospital_dataset
                WHERE {where_clause} AND plan_name IS NOT NULL
                GROUP BY plan_name
                ORDER BY plan_name;
            '''
            print("SQL:", query)
            print("Params:", params)
            cur.execute(query, params)
            plans = [{"name": row['plan_name'], "count": row['count']} for row in cur.fetchall()]
            print("Plans fetched:", plans)
        return jsonify(plans)
    except Exception as e:
        import traceback
        print("Error in /api/filters/plans:", traceback.format_exc())
        print("Exception message:", str(e))
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000) 