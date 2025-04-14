from .base_strategy import BaseIngestionStrategy
import json
import logging
import os
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, explode, array_join, lit, coalesce, greatest, least
import traceback
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from json_processor import JSONProcessor

logger = logging.getLogger(__name__)

class Type3IngestionStrategy(BaseIngestionStrategy):
    def __init__(self):
        super().__init__()
        self.strategy_name = "type3"
        self.description = "JSON Direct Ingestion Strategy"
        self.processor = None
        
        # Define the expected JSON schema
        self.expected_schema = {
            "required": [
                "hospital_name",
                "last_updated_on",
                "version",
                "hospital_location",
                "hospital_address",
                "standard_charge_information"
            ]
        }

    def process_csv(self, file_path):
        """
        Not used in Type 3 strategy as it handles JSON files.
        Implemented to satisfy abstract method requirement.
        """
        raise NotImplementedError("Type 3 strategy does not process CSV files")

    def read_json(self, file_path):
        """
        Read and validate JSON file.
        
        Args:
            file_path (str): Path to the JSON file
            
        Returns:
            dict: Parsed JSON data
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            logger.error(f"Error reading JSON file: {str(e)}")
            raise

    def validate_file(self, file_path):
        """
        Validate that the file is a proper JSON file with required structure and fields.
        
        Args:
            file_path (str): Path to the JSON file
            
        Returns:
            tuple: (bool, str) - (is_valid, error_message)
        """
        try:
            if not os.path.exists(file_path):
                return False, f"File not found: {file_path}"
                
            data = self.read_json(file_path)
                
            # Validate basic structure
            if not isinstance(data, dict):
                return False, "JSON root must be an object"
                
            # Validate required fields
            for field in self.expected_schema["required"]:
                if field not in data:
                    return False, f"Missing required field: {field}"
            
            # Validate arrays
            if not isinstance(data["hospital_location"], list):
                return False, "hospital_location must be an array"
                
            if not isinstance(data["hospital_address"], list):
                return False, "hospital_address must be an array"
                
            if not isinstance(data["standard_charge_information"], list):
                return False, "standard_charge_information must be an array"
                
            # Validate charge information
            for idx, charge in enumerate(data["standard_charge_information"]):
                if "description" not in charge:
                    return False, f"Missing description in charge at index {idx}"
                    
                if "code_information" not in charge or not isinstance(charge["code_information"], list):
                    return False, f"Missing or invalid code_information in charge at index {idx}"
                    
                if "standard_charges" not in charge or not isinstance(charge["standard_charges"], list):
                    return False, f"Missing or invalid standard_charges in charge at index {idx}"
                    
                # Validate code information
                for code_idx, code_info in enumerate(charge["code_information"]):
                    if "code" not in code_info or "type" not in code_info:
                        return False, f"Missing code or type in code_information at charge {idx}, code {code_idx}"
                
                # Validate standard charges - now more lenient with minimum/maximum
                for charge_idx, std_charge in enumerate(charge["standard_charges"]):
                    # Check if at least some pricing information is available
                    has_pricing = False
                    for field in ["minimum", "maximum", "gross", "discounted", "standard"]:
                        if field in std_charge:
                            has_pricing = True
                            try:
                                value = float(std_charge[field])
                                if value < 0:
                                    return False, f"Negative value not allowed for {field} at charge {idx}, item {charge_idx}"
                            except (ValueError, TypeError):
                                return False, f"Invalid numeric value for {field} at charge {idx}, item {charge_idx}"
                    
                    if not has_pricing:
                        return False, f"No valid pricing information found in standard_charges at charge {idx}, item {charge_idx}"
            
            return True, "Validation successful"
            
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON format: {str(e)}"
        except Exception as e:
            logger.error(f"Error validating JSON file: {str(e)}")
            logger.error(traceback.format_exc())
            return False, f"Validation error: {str(e)}"

    def process_file(self, file_path, spark=None):
        """
        Process the JSON file and transform data into required format.
        
        Args:
            file_path (str): Path to the JSON file
            spark (SparkSession, optional): Spark session for processing
            
        Returns:
            tuple: (hospital_info, charges_df) - Processed hospital info and charges data
        """
        try:
            if not spark:
                spark = SparkSession.builder \
                    .appName("Hospital JSON Processing") \
                    .config("spark.driver.memory", "4g") \
                    .config("spark.executor.memory", "4g") \
                    .getOrCreate()
            
            # Read JSON file
            df = spark.read \
                .option("multiline", "true") \
                .option("mode", "PERMISSIVE") \
                .json(file_path)
            
            # Create hospital information dataframe
            hospital_info_df = df.select(
                col("hospital_name"),
                col("last_updated_on"),
                col("version"),
                array_join("hospital_location", ", ").alias("hospital_location"),
                array_join("hospital_address", ", ").alias("hospital_address")
            ).cache()
            
            # Get hospital info as dictionary
            hospital_info = hospital_info_df.first().asDict()
            
            # First level explosion: standard_charge_information
            charge_info_df = df.select(
                col("hospital_name"),
                explode("standard_charge_information").alias("charge_info")
            )
            
            # Second level explosion: standard_charges
            charges_df = charge_info_df.select(
                col("hospital_name"),
                col("charge_info.description"),
                col("charge_info.code_information")[0].getField("code").alias("code"),
                col("charge_info.code_information")[0].getField("type").alias("code_type"),
                explode("charge_info.standard_charges").alias("charges")
            )
            
            # Final dataframe with all required fields
            final_charges_df = charges_df.select(
                col("hospital_name"),
                col("description"),
                col("code"),
                col("code_type"),
                # Use coalesce to handle missing fields and provide fallbacks
                coalesce(
                    col("charges.gross"),
                    col("charges.standard"),
                    lit(0)
                ).cast("decimal(20,2)").alias("standard_charge_gross"),
                coalesce(
                    col("charges.discounted"),
                    lit(0)
                ).cast("decimal(20,2)").alias("standard_charge_negotiated_dollar"),
                # For minimum, use the least non-null value among available price fields
                coalesce(
                    col("charges.minimum"),
                    least(
                        coalesce(col("charges.gross"), lit(0)),
                        coalesce(col("charges.discounted"), lit(0)),
                        coalesce(col("charges.standard"), lit(0))
                    )
                ).cast("decimal(20,2)").alias("standard_charge_min"),
                # For maximum, use the greatest non-null value among available price fields
                coalesce(
                    col("charges.maximum"),
                    greatest(
                        coalesce(col("charges.gross"), lit(0)),
                        coalesce(col("charges.discounted"), lit(0)),
                        coalesce(col("charges.standard"), lit(0))
                    )
                ).cast("decimal(20,2)").alias("standard_charge_max"),
                lit(True).alias("is_active"),
                lit(datetime.now()).alias("created_at"),
                lit(datetime.now()).alias("updated_at")
            ).repartition(10).cache()
            
            return hospital_info, final_charges_df
            
        except Exception as e:
            logger.error(f"Error processing JSON file: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if spark and not self.processor:
                spark.stop()

    def get_example_format(self):
        """
        Return an example of the expected JSON format.
        
        Returns:
            str: Example JSON format
        """
        example = {
            "hospital_name": "Example Hospital",
            "last_updated_on": "2024-03-20",
            "version": "1.0",
            "hospital_location": ["City", "State"],
            "hospital_address": ["123 Healthcare Ave", "City, State 12345"],
            "standard_charge_information": [
                {
                    "description": "Example Medical Service",
                    "code_information": [
                        {
                            "code": "12345",
                            "type": "CPT"
                        }
                    ],
                    "standard_charges": [
                        {
                            "minimum": 600.00,  # Optional
                            "maximum": 1200.00,  # Optional
                            "gross": 1000.00,   # Optional
                            "discounted": 800.00,  # Optional
                            "setting": "inpatient",
                            "payers_information": [
                                {
                                    "payer": "Example Insurance",
                                    "plan": "Standard Plan"
                                }
                            ],
                            "billing_class": "facility"
                        }
                    ]
                }
            ]
        }
        return json.dumps(example, indent=2) 