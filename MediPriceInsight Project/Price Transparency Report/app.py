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
                'region': get_distinct_values(cur, 'region'),
                'city': get_distinct_values(cur, 'city'),
                'code': get_distinct_values(cur, 'code'),
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

@app.route('/api/regions')
@cache.cached(timeout=300)  # Cache for 5 minutes
def get_regions():
    start_time = time.time()
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get distinct regions
            query = """
                SELECT DISTINCT region
                FROM public.hospital_dataset
                WHERE region IS NOT NULL
                ORDER BY region
            """
            cur.execute(query)
            results = cur.fetchall()
            
            duration = time.time() - start_time
            logger.info(f"Regions fetched in {duration:.2f}s - {len(results)} regions")
            
            # Return just the array of regions
            return jsonify([row['region'] for row in results])

    except Exception as e:
        logger.error(f"Error fetching regions: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/cities')
def get_cities():
    start_time = time.time()
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get region parameter
            region = request.args.get('region')
            
            # Build query based on whether region is provided
            if region:
                query = """
                    SELECT DISTINCT city
                    FROM public.hospital_dataset
                    WHERE region = %s AND city IS NOT NULL
                    ORDER BY city
                """
                cur.execute(query, (region,))
            else:
                query = """
                    SELECT DISTINCT city
                    FROM public.hospital_dataset
                    WHERE city IS NOT NULL
                    ORDER BY city
                """
                cur.execute(query)
            
            results = cur.fetchall()
            
            duration = time.time() - start_time
            logger.info(f"Cities fetched in {duration:.2f}s - {len(results)} cities")
            
            # Return just the array of cities
            return jsonify([row['city'] for row in results])

    except Exception as e:
        logger.error(f"Error fetching cities: {str(e)}")
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
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get filter parameters
            region = request.args.get('region')
            city = request.args.get('city')
            code = request.args.get('code')
            payer_name = request.args.get('payer_name')
            plan_name = request.args.get('plan_name')
            
            # Validate required parameters
            if not region or not city or not code:
                return jsonify({
                    'status': 'error',
                    'message': 'Region, city, and code are required parameters',
                    'data': []
                }), 400
            
            # Build base WHERE clause with required parameters
            base_conditions = [
                "region = %s",
                "city = %s",
                "code = %s"
            ]
            base_params = [region, city, code]

            # Get unique payer names and plan names first
            unique_payers_query = f"""
                SELECT DISTINCT payer_name
                FROM public.hospital_dataset
                WHERE {" AND ".join(base_conditions)}
                ORDER BY payer_name
            """
            cur.execute(unique_payers_query, base_params)
            unique_payers = [row['payer_name'] for row in cur.fetchall()]

            unique_plans_query = f"""
                SELECT DISTINCT plan_name
                FROM public.hospital_dataset
                WHERE {" AND ".join(base_conditions)}
                ORDER BY plan_name
            """
            cur.execute(unique_plans_query, base_params)
            unique_plans = [row['plan_name'] for row in cur.fetchall()]

            # Build final WHERE clause with all filters
            conditions = base_conditions.copy()
            params = base_params.copy()

            # Add payer name filter if provided
            if payer_name:
                conditions.append("payer_name = %s")
                params.append(payer_name)

            # Add plan name filter if provided
            if plan_name:
                conditions.append("plan_name = %s")
                params.append(plan_name)

            where_clause = " AND ".join(conditions)

            # Get total count
            count_query = f"""
                SELECT COUNT(*) as count
                FROM public.hospital_dataset
                WHERE {where_clause}
            """
            cur.execute(count_query, params)
            total_count = cur.fetchone()['count']

            # Get data with all relevant fields
            if total_count > 0:
                data_query = f"""
                    SELECT 
                        id,
                        hospital_name,
                        hospital_address,
                        code,
                        description,
                        city,
                        region,
                        payer_name,
                        plan_name,
                        standard_charge_min,
                        standard_charge_max,
                        standard_charge_gross,
                        standard_charge_negotiated_dollar
                    FROM public.hospital_dataset
                    WHERE {where_clause}
                    ORDER BY 
                        hospital_name,
                        payer_name,
                        plan_name
                """
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
                'unique_payers': unique_payers,
                'unique_plans': unique_plans
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
    start_time = time.time()
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get filter parameters
            region = request.args.get('region')
            city = request.args.get('city')
            code = request.args.get('code')
            
            # Validate required parameters
            if not region or not city:
                return jsonify({
                    'status': 'error',
                    'message': 'Region and city are required parameters',
                    'data': []
                }), 400
            
            # Build WHERE clause with region and city
            conditions = [
                "region = %s",
                "city = %s"
            ]
            params = [region, city]

            # If specific code is requested, add it to the filter
            if code:
                conditions.append("code = %s")
                params.append(code)

            where_clause = " AND ".join(conditions)

            # Get distinct codes and descriptions for the selected region and city
            query = f"""
                SELECT DISTINCT code, description 
                FROM public.hospital_dataset 
                WHERE {where_clause}
                ORDER BY code
            """
            
            cur.execute(query, params)
            results = cur.fetchall()
            
            duration = time.time() - start_time
            logger.info(f"Codes fetched in {duration:.2f}s - {len(results)} unique codes for {region}, {city}")
            
            return jsonify({
                'status': 'success',
                'data': results,
                'count': len(results)
            })

    except Exception as e:
        logger.error(f"Error fetching codes: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'Failed to fetch codes',
            'error': str(e)
        }), 500
    finally:
        if conn:
            conn.close()

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

if __name__ == '__main__':
    app.run(debug=True, port=5000) 