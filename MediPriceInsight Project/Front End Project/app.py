import csv
from io import StringIO
from functools import lru_cache
import time
from flask import Flask, render_template, request, flash, redirect, url_for, jsonify, make_response
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from config import DB_CONFIG
import threading
import numpy as np

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

# Connection pool configuration
DB_POOL = None
DB_POOL_LOCK = threading.Lock()

# Add initialization flag
_initialized = False

def init_db_pool():
    """Initialize the database connection pool"""
    global DB_POOL
    if DB_POOL is None:
        with DB_POOL_LOCK:
            if DB_POOL is None:
                try:
                    DB_POOL = pool.ThreadedConnectionPool(
                        minconn=1,
                        maxconn=20,
                        host=DB_CONFIG['host'],
                        database=DB_CONFIG['database'],
                        user=DB_CONFIG['user'],
                        password=DB_CONFIG['password'],
                        port=DB_CONFIG['port']
                    )
                except Exception as e:
                    print(f"Error creating connection pool: {e}")
                    return None
    return DB_POOL

def get_db_connection():
    """Get a connection from the pool"""
    pool = init_db_pool()
    if pool is None:
        return None
    try:
        return pool.getconn()
    except Exception as e:
        print(f"Error getting connection from pool: {e}")
        return None

def release_db_connection(conn):
    """Release a connection back to the pool"""
    if conn and DB_POOL:
        try:
            DB_POOL.putconn(conn)
        except Exception as e:
            print(f"Error releasing connection: {e}")

# Application-level cache
class DataCache:
    def __init__(self):
        self._cache = {}
        self._lock = threading.Lock()
    
    def get(self, key):
        with self._lock:
            return self._cache.get(key)
    
    def set(self, key, value, ttl=300):  # 5 minutes default TTL
        with self._lock:
            self._cache[key] = {
                'value': value,
                'expires': time.time() + ttl
            }
    
    def is_valid(self, key):
        with self._lock:
            if key not in self._cache:
                return False
            return time.time() < self._cache[key]['expires']
    
    def clear(self):
        with self._lock:
            self._cache.clear()

# Initialize cache
data_cache = DataCache()

@app.before_request
def before_request():
    """Initialize the application before each request if not already initialized"""
    if not _initialized:
        init_app()

@lru_cache(maxsize=1)
def get_cities():
    """Fetch distinct cities from the database with caching"""
    cache_key = 'cities'
    if data_cache.is_valid(cache_key):
        return data_cache.get(cache_key)['value']
    
    try:
        conn = get_db_connection()
        if conn is None:
            return []
        
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM get_distinct_cities();")
            cities = [row[0] for row in cur.fetchall()]
        
        data_cache.set(cache_key, cities)
        return cities
    except Exception as e:
        print(f"Error fetching cities: {str(e)}")
        return []
    finally:
        release_db_connection(conn)

@lru_cache(maxsize=1)
def get_regions():
    """Fetch distinct regions from the database with caching"""
    cache_key = 'regions'
    if data_cache.is_valid(cache_key):
        return data_cache.get(cache_key)['value']
    
    try:
        conn = get_db_connection()
        if conn is None:
            return []
        
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM get_distinct_regions();")
            regions = [row[0] for row in cur.fetchall()]
        
        data_cache.set(cache_key, regions)
        return regions
    except Exception as e:
        print(f"Error fetching regions: {str(e)}")
        return []
    finally:
        release_db_connection(conn)

@lru_cache(maxsize=1)
def get_all_codes_and_descriptions():
    """Fetch all distinct codes and descriptions from the database with caching"""
    cache_key = 'codes_descriptions'
    if data_cache.is_valid(cache_key):
        return data_cache.get(cache_key)['value']
    
    try:
        conn = get_db_connection()
        if conn is None:
            return []
        
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM get_codes_and_descriptions();")
            results = cur.fetchall()
            matches = [{"code": row[0], "description": row[1]} for row in results]
        
        data_cache.set(cache_key, matches)
        return matches
    except Exception as e:
        print(f"Error fetching all codes and descriptions: {str(e)}")
        return []
    finally:
        release_db_connection(conn)

@lru_cache(maxsize=100)
def get_cities_by_region(region):
    """Fetch distinct cities for a specific region from the database with caching"""
    cache_key = f'cities_region_{region}'
    if data_cache.is_valid(cache_key):
        return data_cache.get(cache_key)['value']
    
    try:
        conn = get_db_connection()
        if conn is None:
            return []
        
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM get_cities_for_region(%s);", (region,))
            cities = [row[0] for row in cur.fetchall()]
        
        data_cache.set(cache_key, cities)
        return cities
    except Exception as e:
        print(f"Error fetching cities by region: {str(e)}")
        return []
    finally:
        release_db_connection(conn)

@lru_cache(maxsize=100)
def search_codes_and_descriptions(search_term):
    """Search for codes and descriptions based on a search term with caching"""
    cache_key = f'search_{search_term}'
    if data_cache.is_valid(cache_key):
        return data_cache.get(cache_key)['value']
    
    try:
        conn = get_db_connection()
        if conn is None:
            return []
        
        with conn.cursor() as cur:
            # Create search patterns with proper wildcards
            search_pattern = f"%{search_term}%"
            cur.execute("""
                SELECT DISTINCT code, description
                FROM public.hospital_dataset 
                WHERE code ILIKE %s 
                   OR description ILIKE %s
                ORDER BY code
                LIMIT 10;
            """, (search_pattern, search_pattern))
            results = cur.fetchall()
            matches = [{"code": row[0], "description": row[1]} for row in results]
        
        data_cache.set(cache_key, matches)
        return matches
    except Exception as e:
        print(f"Error searching codes and descriptions: {str(e)}")
        return []
    finally:
        release_db_connection(conn)

def get_hospital_data_by_code(code, city=None, region=None):
    """Fetch hospital data filtered by code and optional city/region with optimized query"""
    cache_key = f'hospital_data_{code}_{city}_{region}'
    if data_cache.is_valid(cache_key):
        return data_cache.get(cache_key)['value']
    
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT * FROM get_hospital_data(%s, %s, %s);", (code, city, region))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            data = {'columns': columns, 'data': [dict(row) for row in rows]}
        
        data_cache.set(cache_key, data)
        return data
    except Exception as e:
        print(f"Error fetching hospital data: {str(e)}")
        return None
    finally:
        release_db_connection(conn)

@app.route('/')
def index():
    """Render the main page without initial data"""
    return render_template('index.html')

@app.route('/api/initial-data')
def get_initial_data():
    """API endpoint to get all initial data in one request"""
    try:
        print("Fetching initial data...")
        
        # Get cities
        cities = get_cities()
        if not cities:
            raise ValueError("No cities found in database")
        print(f"Fetched {len(cities)} cities")
        
        # Get regions
        regions = get_regions()
        if not regions:
            raise ValueError("No regions found in database")
        print(f"Fetched {len(regions)} regions")
        
        # Get codes and descriptions
        codes_and_descriptions = get_all_codes_and_descriptions()
        if not codes_and_descriptions:
            raise ValueError("No codes and descriptions found in database")
        print(f"Fetched {len(codes_and_descriptions)} codes and descriptions")
        
        # Get payer names
        conn = get_db_connection()
        payer_names = []
        plan_names = []
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM get_distinct_payer_names();")
                    payer_names = [row[0] for row in cur.fetchall()]
                    cur.execute("SELECT * FROM get_distinct_plan_names();")
                    plan_names = [row[0] for row in cur.fetchall()]
                print(f"Fetched {len(payer_names)} payer names")
                print(f"Fetched {len(plan_names)} plan names")
            except Exception as e:
                print(f"Error fetching payer names: {str(e)}")
            finally:
                release_db_connection(conn)

        return jsonify({
            'cities': cities,
            'regions': regions,
            'codes_and_descriptions': codes_and_descriptions,
            'payer_names': payer_names,
            'plan_names': plan_names
        })
    except Exception as e:
        print(f"Error fetching initial data: {str(e)}")
        return jsonify({
            'error': str(e),
            'cities': [],
            'regions': [],
            'codes_and_descriptions': [],
            'payer_names': [],
            'plan_names': []
        }), 500

@app.route('/search')
def search():
    """Handle search requests for codes and descriptions"""
    try:
        search_term = request.args.get('term', '')
        print(f"Search term received: {search_term}")  # Debug log
        
        if not search_term or len(search_term) < 2:
            return jsonify([])
        
        results = search_codes_and_descriptions(search_term)
        print(f"Search results: {results}")  # Debug log
        return jsonify(results)
    except Exception as e:
        print(f"Error in search endpoint: {str(e)}")  # Debug log
        return jsonify([])

@app.route('/get_data')
def get_data_multi():
    """API endpoint to get hospital data with multi-select filters and column summaries."""
    try:
        code_list = request.args.getlist('code')
        city_list = request.args.getlist('city')
        region_list = request.args.getlist('region')
        payer_name_list = request.args.getlist('payer_name')
        plan_name_list = request.args.getlist('plan_name')
        # If any are empty or 'all', treat as no filter
        code_list = [c for c in code_list if c and c != 'all']
        city_list = [c for c in city_list if c and c != 'all']
        region_list = [r for r in region_list if r and r != 'all']
        payer_name_list = [p for p in payer_name_list if p and p != 'all']
        plan_name_list = [p for p in plan_name_list if p and p != 'all']
        data = get_hospital_data_multi(
            code_list or None, city_list or None, region_list or None, payer_name_list or None, plan_name_list or None
        )
        if data:
            return jsonify(data)
        return jsonify({'error': 'Failed to fetch data'}), 500
    except Exception as e:
        print(f"Error in get_data_multi endpoint: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/get_cities/<region>')
def get_cities_by_region_route(region):
    """API endpoint to get cities for a specific region"""
    try:
        cities = get_cities_by_region(region)
        return jsonify(cities)
    except Exception as e:
        print(f"Error in get_cities endpoint: {str(e)}")
        return jsonify([])

@app.route('/generate_report')
def generate_report_multi():
    """Generate and download a CSV report for the selected codes and filters (multi-select)."""
    try:
        code_list = request.args.getlist('code')
        city_list = request.args.getlist('city')
        region_list = request.args.getlist('region')
        code_list = [c for c in code_list if c and c != 'all']
        city_list = [c for c in city_list if c and c != 'all']
        region_list = [r for r in region_list if r and r != 'all']
        data = get_hospital_data_multi(code_list or None, city_list or None, region_list or None)
        if not data:
            return jsonify({'error': 'No data found'}), 404
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(data['columns'])
        for row in data['data']:
            writer.writerow([row[col] for col in data['columns']])
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers["Content-Disposition"] = f"attachment; filename=hospital_report.csv"
        response.headers["Content-type"] = "text/csv"
        return response
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        return jsonify({'error': 'Failed to generate report'}), 500

def get_hospital_data_multi(code_list=None, city_list=None, region_list=None, payer_name_list=None, plan_name_list=None):
    """Fetch hospital data filtered by multiple codes, cities, regions, payer names, and plan names. Also compute column summaries."""
    cache_key = f"hospital_data_multi_{'_'.join(code_list or [])}_{'_'.join(city_list or [])}_{'_'.join(region_list or [])}_{'_'.join(payer_name_list or [])}_{'_'.join(plan_name_list or [])}"
    if data_cache.is_valid(cache_key):
        return data_cache.get(cache_key)['value']
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            wheres = []
            params = []
            if code_list:
                wheres.append(f"code = ANY(%s)")
                params.append(code_list)
            if city_list:
                wheres.append(f"city = ANY(%s)")
                params.append(city_list)
            if region_list:
                wheres.append(f"region = ANY(%s)")
                params.append(region_list)
            if payer_name_list:
                wheres.append(f"payer_name = ANY(%s)")
                params.append(payer_name_list)
            if plan_name_list:
                wheres.append(f"plan_name = ANY(%s)")
                params.append(plan_name_list)
            where_clause = f"WHERE {' AND '.join(wheres)}" if wheres else ''
            sql = f"SELECT * FROM public.hospital_dataset {where_clause} ORDER BY hospital_name;"
            cur.execute(sql, params)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            data_rows = [dict(row) for row in rows]
            # Compute column summaries
            summaries = {}
            arr = {col: [row[col] for row in data_rows] for col in columns} if data_rows else {col: [] for col in columns}
            for col in columns:
                col_data = arr[col]
                # Numeric columns: histogram
                if all(isinstance(x, (int, float, np.integer, np.floating)) or x is None for x in col_data):
                    clean_data = [x for x in col_data if x is not None]
                    if clean_data:
                        hist, bin_edges = np.histogram(clean_data, bins=10)
                        summaries[col] = {
                            'histogram': hist.tolist(),
                            'bin_edges': bin_edges.tolist(),
                            'unique_count': len(set(clean_data))
                        }
                    else:
                        summaries[col] = {'histogram': [], 'bin_edges': [], 'unique_count': 0}
                else:
                    # Categorical columns: unique count
                    clean_data = [x for x in col_data if x is not None]
                    summaries[col] = {'unique_count': len(set(clean_data))}
            data = {'columns': columns, 'data': data_rows, 'summaries': summaries}
        data_cache.set(cache_key, data)
        return data
    except Exception as e:
        print(f"Error fetching hospital data (multi): {str(e)}")
        return None
    finally:
        release_db_connection(conn)

@app.route('/api/filter-options')
def get_filter_options():
    """API endpoint to get valid filter options for cascading dropdowns based on current selections, including counts for each option."""
    try:
        code_list = request.args.getlist('code')
        region_list = request.args.getlist('region')
        city_list = request.args.getlist('city')
        payer_name_list = request.args.getlist('payer_name')
        plan_name_list = request.args.getlist('plan_name')
        code_list = [c for c in code_list if c and c != 'all']
        region_list = [r for r in region_list if r and r != 'all']
        city_list = [c for c in city_list if c and c != 'all']
        payer_name_list = [p for p in payer_name_list if p and p != 'all']
        plan_name_list = [p for p in plan_name_list if p and p != 'all']
        conn = get_db_connection()
        if conn is None:
            return jsonify({'regions': [], 'cities': [], 'payer_names': [], 'plan_names': []})
        with conn.cursor() as cur:
            wheres = []
            params = []
            if code_list:
                wheres.append("code = ANY(%s)")
                params.append(code_list)
            if region_list:
                wheres.append("region = ANY(%s)")
                params.append(region_list)
            if city_list:
                wheres.append("city = ANY(%s)")
                params.append(city_list)
            if payer_name_list:
                wheres.append("payer_name = ANY(%s)")
                params.append(payer_name_list)
            if plan_name_list:
                wheres.append("plan_name = ANY(%s)")
                params.append(plan_name_list)
            where_clause = f"WHERE {' AND '.join(wheres)}" if wheres else ''
            # Get filtered regions with counts
            cur.execute(f"SELECT region, COUNT(*) FROM public.hospital_dataset {where_clause} GROUP BY region ORDER BY region;", params)
            regions = [{"name": row[0], "count": row[1]} for row in cur.fetchall() if row[0]]
            # Get filtered cities with counts
            print(f"SQL for cities: SELECT city, COUNT(*) FROM public.hospital_dataset {where_clause} GROUP BY city ORDER BY city;")
            print(f"Params: {params}")
            cur.execute(f"SELECT city, COUNT(*) FROM public.hospital_dataset {where_clause} GROUP BY city ORDER BY city;", params)
            cities = [{"name": row[0], "count": row[1]} for row in cur.fetchall() if row[0]]
            # Get filtered payer_names with counts
            cur.execute(f"SELECT payer_name, COUNT(*) FROM public.hospital_dataset {where_clause} GROUP BY payer_name ORDER BY payer_name;", params)
            payer_names = [{"name": row[0], "count": row[1]} for row in cur.fetchall() if row[0]]
            # Get filtered plan_names with counts
            cur.execute(f"SELECT plan_name, COUNT(*) FROM public.hospital_dataset {where_clause} GROUP BY plan_name ORDER BY plan_name;", params)
            plan_names = [{"name": row[0], "count": row[1]} for row in cur.fetchall() if row[0]]
        release_db_connection(conn)
        return jsonify({
            'regions': regions,
            'cities': cities,
            'payer_names': payer_names,
            'plan_names': plan_names
        })
    except Exception as e:
        print(f"Error in get_filter_options endpoint: {str(e)}")
        return jsonify({'regions': [], 'cities': [], 'payer_names': [], 'plan_names': []}), 500

def init_app():
    """Initialize the application before first request"""
    global _initialized
    if not _initialized:
        conn = get_db_connection()
        if conn:
            try:
                # Verify prepared statements exist
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT name FROM pg_prepared_statements;
                    """)
                    prepared_statements = [row[0] for row in cur.fetchall()]
                    print("Available prepared statements:", prepared_statements)
                
                _initialized = True
                print("Application initialized successfully")
            except Exception as e:
                print(f"Error initializing application: {e}")
                _initialized = False
            finally:
                release_db_connection(conn)
        else:
            print("Failed to get database connection for initialization")
            _initialized = False

if __name__ == '__main__':
    app.run(debug=True) 