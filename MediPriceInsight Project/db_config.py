"""
Database configuration settings for MediPriceInsight Project
"""

# PostgreSQL Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'healthcarepoc',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

# Connection string format
def get_connection_string():
    """Returns the formatted connection string for PostgreSQL"""
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# Table Names
TABLES = {
    'hospital_info': 'hospital_info',
    'charge_data': 'charge_data',
    'pivot_data': 'pivot_data',
    'json_data': 'json_data',
    'audit_log': 'audit_log'
}

# Schema Definitions
SCHEMAS = {
    'hospital_info': """
        CREATE TABLE IF NOT EXISTS hospital_info (
            hospital_id SERIAL PRIMARY KEY,
            hospital_name VARCHAR(255) NOT NULL,
            address VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(50),
            zip_code VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    'charge_data': """
        CREATE TABLE IF NOT EXISTS charge_data (
            charge_id SERIAL PRIMARY KEY,
            hospital_id INTEGER REFERENCES hospital_info(hospital_id),
            procedure_code VARCHAR(50),
            procedure_description TEXT,
            gross_charge DECIMAL(12,2),
            discounted_cash_price DECIMAL(12,2),
            min_negotiated_rate DECIMAL(12,2),
            max_negotiated_rate DECIMAL(12,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    'pivot_data': """
        CREATE TABLE IF NOT EXISTS pivot_data (
            pivot_id SERIAL PRIMARY KEY,
            hospital_id INTEGER REFERENCES hospital_info(hospital_id),
            payer_name VARCHAR(255),
            plan_name VARCHAR(255),
            procedure_code VARCHAR(50),
            negotiated_rate DECIMAL(12,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    'json_data': """
        CREATE TABLE IF NOT EXISTS json_data (
            json_id SERIAL PRIMARY KEY,
            hospital_id INTEGER REFERENCES hospital_info(hospital_id),
            data_content JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    'audit_log': """
        CREATE TABLE IF NOT EXISTS audit_log (
            log_id SERIAL PRIMARY KEY,
            action_type VARCHAR(50),
            table_name VARCHAR(50),
            record_id INTEGER,
            action_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_id VARCHAR(100),
            details TEXT
        )
    """
}

# Database Indexes
INDEXES = {
    'hospital_info_name_idx': 'CREATE INDEX IF NOT EXISTS hospital_info_name_idx ON hospital_info(hospital_name)',
    'charge_data_procedure_idx': 'CREATE INDEX IF NOT EXISTS charge_data_procedure_idx ON charge_data(procedure_code)',
    'pivot_data_payer_idx': 'CREATE INDEX IF NOT EXISTS pivot_data_payer_idx ON pivot_data(payer_name)',
    'json_data_hospital_idx': 'CREATE INDEX IF NOT EXISTS json_data_hospital_idx ON json_data(hospital_id)',
    'audit_log_action_idx': 'CREATE INDEX IF NOT EXISTS audit_log_action_idx ON audit_log(action_type, table_name)'
}

# Query Templates
QUERIES = {
    'insert_hospital': """
        INSERT INTO hospital_info (hospital_name, address, city, state, zip_code)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING hospital_id
    """,
    'insert_charge': """
        INSERT INTO charge_data (
            hospital_id, procedure_code, procedure_description,
            gross_charge, discounted_cash_price,
            min_negotiated_rate, max_negotiated_rate
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """,
    'insert_pivot': """
        INSERT INTO pivot_data (
            hospital_id, payer_name, plan_name,
            procedure_code, negotiated_rate
        )
        VALUES (%s, %s, %s, %s, %s)
    """,
    'insert_json': """
        INSERT INTO json_data (hospital_id, data_content)
        VALUES (%s, %s)
    """,
    'insert_audit': """
        INSERT INTO audit_log (action_type, table_name, record_id, user_id, details)
        VALUES (%s, %s, %s, %s, %s)
    """
}

# Error Messages
ERROR_MESSAGES = {
    'connection_error': 'Failed to connect to the database. Please check your connection settings.',
    'insert_error': 'Error inserting data into {}. Error: {}',
    'query_error': 'Error executing query: {}',
    'schema_error': 'Error creating schema: {}'
} 