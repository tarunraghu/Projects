from flask import jsonify
import os
import logging
import traceback
from json_processor import JSONProcessor
from pyspark.sql.functions import explode, col, current_timestamp

logger = logging.getLogger(__name__)

def process_hospital_charges_type3(data_file, hospital_name, task_id, user_name='system', ingestion_type='type3', chunk_size=50000, task_results=None):
    """Process Type 3 JSON file for hospital charges"""
    processor = None
    try:
        # Ensure the file exists and is accessible
        if not os.path.exists(data_file):
            return jsonify({
                'error': 'File not found',
                'details': {
                    'validation_errors': [f"File not found: {data_file}"],
                    'available_fields': []
                }
            }), 400
            
        # Initialize JSON processor
        processor = JSONProcessor()
        
        # Use the uploads directory directly
        output_dir = os.path.dirname(data_file)
        
        # Process the JSON file
        hospital_info_csv, charges_csv, hospital_address_data = processor.process_json_file(data_file, output_dir)
        
        # Update task progress
        if task_results and task_id in task_results:
            task = task_results[task_id]
            task.progress = 100
            task.message = 'JSON file processed successfully'
            task.status = 'SUCCESS'
        
        return jsonify({
            'success': True,
            'message': 'File processed successfully',
            'hospital_info_csv': hospital_info_csv,
            'charges_csv': charges_csv,
            'hospital_address_data': hospital_address_data
        })
        
    except Exception as e:
        logger.error(f"Error processing JSON file: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Update task status
        if task_results and task_id in task_results:
            task = task_results[task_id]
            task.status = 'FAILURE'
            task.error = str(e)
        
        return jsonify({
            'error': 'Error processing JSON file',
            'details': {
                'validation_errors': [str(e)],
                'available_fields': []
            }
        }), 400
    finally:
        if processor:
            processor.stop_spark() 