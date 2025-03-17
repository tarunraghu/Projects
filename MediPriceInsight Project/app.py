from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import traceback
import logging
import psycopg2
from pyspark.sql.types import StringType, FloatType

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "dbname": "healthcarepoc",
    "user": "postgres",
    "password": "Consis10C!",
    "host": "localhost",
    "port": "5432"
}

def initialize_spark():
    try:
        return SparkSession.builder \
            .appName("Healthcare Data Processing") \
            .config("spark.jars", "postgresql-42.7.2.jar") \
            .getOrCreate()
    except Exception as e:
        logger.error(f"Error initializing Spark: {str(e)}")
        raise

def save_to_postgres(df, table_name="billing_data"):
    """
    Save Spark DataFrame to PostgreSQL
    """
    try:
        # Check if DataFrame is empty
        row_count = df.count()
        if row_count == 0:
            logger.warning("Empty DataFrame. No records will be saved to the database.")
            raise ValueError("No data was found to save. Please check if your input file contains the correct data.")

        # Get unique hospital and branch combination from the new data
        hospital_info = df.select("hospital_name", "hospital_branch").distinct().collect()
        hospital_name = hospital_info[0]["hospital_name"]
        hospital_branch = hospital_info[0]["hospital_branch"]

        # Create a temporary connection to check existing data
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Check if the hospital and branch combination already exists
        cur.execute("""
            SELECT COUNT(*) FROM billing_data 
            WHERE hospital_name = %s AND hospital_branch = %s
        """, (hospital_name, hospital_branch))
        
        count = cur.fetchone()[0]
        cur.close()
        conn.close()

        if count > 0:
            logger.warning(f"Data for hospital '{hospital_name}' and branch '{hospital_branch}' already exists.")
            raise ValueError(
                f"⚠️ Duplicate Entry Detected\n\n"
                f"The system found existing records for:\n"
                f"Hospital: {hospital_name}\n"
                f"Branch: {hospital_branch}\n\n"
                f"To prevent duplicate data, please either:\n"
                f"1. Use a different hospital/branch combination\n"
                f"2. Contact your administrator if you need to update existing records"
            )

        # Convert all columns to string type to handle potential type mismatches
        for column in ['gross_standard_charges', 'negotiated_standard_charges', 'max_standard_charges', 'min_standard_charges']:
            df = df.withColumn(column, df[column].cast(FloatType()))
        
        # Write to PostgreSQL
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}") \
            .option("dbtable", table_name) \
            .option("user", DB_CONFIG['user']) \
            .option("password", DB_CONFIG['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"Successfully saved {row_count} records to {table_name}")
        return True
    except Exception as e:
        logger.error(f"Error saving to PostgreSQL: {str(e)}")
        raise

@app.route('/')
def index():
    return open('index.html').read()

@app.route('/submit-form', methods=['POST'])
def submit_form():
    try:
        data = request.get_json()
        logging.info(f"Received form data: {data}")
        
        # Validate required fields
        required_fields = [
            'fileType', 'path', 'ingestionStrategy', 'hospitalName', 'hospitalBranch',
            'billingDescField', 'billingCodeField', 'billingCodeValueField',
            'payerName', 'planName', 'grossChargeField', 'negotiatedChargeField',
            'minChargeField', 'maxChargeField'
        ]
        
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return jsonify({
                'success': False,
                'error': f"Missing required fields: {', '.join(missing_fields)}"
            }), 400

        # Validate file type
        if data['fileType'] not in ['csv', 'json']:
            return jsonify({
                'success': False,
                'error': "Invalid file type. Must be 'csv' or 'json'"
            }), 400

        # Validate ingestion strategy
        if data['ingestionStrategy'] != 'type1':
            return jsonify({
                'success': False,
                'error': "Only Type 1 Direct Ingestion is supported"
            }), 400

        # Process the data
        result = process_type1_ingestion(data)
        
        if not result['success']:
            return jsonify({
                'success': False,
                'error': result['error']
            }), 400

        return jsonify({
            'success': True,
            'message': 'Data processed successfully',
            'processed_data': {
                'hospital_info': {
                    'name': data['hospitalName'],
                    'branch': data['hospitalBranch']
                },
                'file_info': {
                    'type': data['fileType'],
                    'path': data['path']
                },
                'fields_processed': [
                    'billingDescField', 'billingCodeField', 'billingCodeValueField',
                    'grossChargeField', 'negotiatedChargeField', 'minChargeField', 'maxChargeField'
                ]
            }
        })

    except Exception as e:
        logging.error(f"Error processing form submission: {str(e)}")
        return jsonify({
            'success': False,
            'error': f"An error occurred while processing the data: {str(e)}"
        }), 500

def process_type1_ingestion(form_data):
    """
    Process the data using Type 1 ingestion strategy with PySpark
    """
    try:
        logger.debug("Starting Type 1 ingestion processing")
        
        # Initialize Spark
        spark = initialize_spark()
        
        # Get form fields
        file_path = form_data['path']
        hospital_name = form_data['hospitalName']
        hospital_branch = form_data['hospitalBranch']
        
        logger.debug(f"Processing file: {file_path}")
        logger.debug(f"Hospital name: {hospital_name}")
        logger.debug(f"Hospital branch: {hospital_branch}")

        try:
            # Load the CSV file
            data_df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(file_path)
        except Exception as e:
            return {
                'success': False,
                'error': f"⚠️ File Access Error\n\nCould not read the file at: {file_path}\n\nPlease check:\n1. The file path is correct\n2. The file exists\n3. You have permission to access it"
            }

        logger.debug("CSV file loaded successfully")

        # Map form field names to CSV column names and validate they exist
        try:
            billing_desc_field = form_data['billingDescField']
            billing_code_field = form_data['billingCodeField']
            billing_code_value_field = form_data['billingCodeValueField']
            payer_name_field = form_data['payerName']
            plan_name_field = form_data['planName']
            gross_charge_field = form_data['grossChargeField']
            negotiated_charge_field = form_data['negotiatedChargeField']
            min_charge_field = form_data['minChargeField']
            max_charge_field = form_data['maxChargeField']

            # Verify all columns exist in the CSV
            csv_columns = data_df.columns
            for field_name, column in [
                ("Billing Description", billing_desc_field),
                ("Billing Code", billing_code_field),
                ("Billing Code Value", billing_code_value_field),
                ("Payer Name", payer_name_field),
                ("Plan Name", plan_name_field),
                ("Gross Charge", gross_charge_field),
                ("Negotiated Charge", negotiated_charge_field),
                ("Minimum Charge", min_charge_field),
                ("Maximum Charge", max_charge_field)
            ]:
                if column not in csv_columns:
                    return {
                        'success': False,
                        'error': f"⚠️ Column Not Found\n\nThe {field_name} column '{column}' was not found in your CSV file.\n\nAvailable columns are:\n{', '.join(csv_columns)}\n\nPlease check your column mappings and try again."
                    }
        except KeyError as e:
            return {
                'success': False,
                'error': f"Missing required field mapping: {str(e)}"
            }

        # Select and rename columns based on form input
        columns_to_include = [
            billing_desc_field,
            billing_code_field,
            billing_code_value_field,
            payer_name_field,
            plan_name_field,
            gross_charge_field,
            negotiated_charge_field,
            max_charge_field,
            min_charge_field
        ]

        # Select specified columns
        df_subset = data_df.select(*columns_to_include)

        # Filter for CPT codes only
        df_filtered = df_subset.filter(df_subset[billing_code_field].contains('CPT'))

        # Check if DataFrame is empty after filtering
        if df_filtered.count() == 0:
            return {
                'success': False,
                'error': f"⚠️ No CPT Codes Found\n\nNo records with CPT codes were found in the column '{billing_code_field}'.\n\nPlease check:\n1. You selected the correct column for billing codes\n2. Your data contains CPT codes\n3. The CPT codes are properly formatted"
            }

        # Rename columns to standardized names
        df_renamed = df_filtered \
            .withColumnRenamed(billing_desc_field, 'billing_description') \
            .withColumnRenamed(billing_code_field, 'billing_code') \
            .withColumnRenamed(billing_code_value_field, 'billing_code_value') \
            .withColumnRenamed(payer_name_field, 'insurance_provider') \
            .withColumnRenamed(plan_name_field, 'insurance_plan') \
            .withColumnRenamed(gross_charge_field, 'gross_standard_charges') \
            .withColumnRenamed(negotiated_charge_field, 'negotiated_standard_charges') \
            .withColumnRenamed(max_charge_field, 'max_standard_charges') \
            .withColumnRenamed(min_charge_field, 'min_standard_charges')

        # Add hospital name column
        df_final = df_renamed.withColumn("hospital_name", lit(hospital_name)) \
                           .withColumn("hospital_branch", lit(hospital_branch))

        # Save to PostgreSQL
        try:
            save_to_postgres(df_final)
            return {
                'success': True,
                'dataframe': df_final
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    except Exception as e:
        logger.error(f"Error in Type 1 ingestion processing: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e)
        }

if __name__ == '__main__':
    app.run(debug=True)