from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, expr, when, lit, current_timestamp, row_number, size, coalesce, array, struct
from pyspark.sql.window import Window
import os
import logging
from datetime import datetime
import traceback
import pandas as pd
import uuid

logger = logging.getLogger(__name__)

class JSONProcessor:
    def __init__(self):
        self.spark = None
        self.initialize_spark()

    def initialize_spark(self):
        """Initialize Spark session with optimized settings"""
        try:
            # Create temp directories for Spark
            spark_temp = os.path.abspath("spark-temp")
            spark_warehouse = os.path.abspath("spark-warehouse")
            os.makedirs(spark_temp, exist_ok=True)
            os.makedirs(spark_warehouse, exist_ok=True)
            
            self.spark = SparkSession.builder \
                .appName("Hospital JSON Processor") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .config("spark.sql.shuffle.partitions", "10") \
                .config("spark.network.timeout", "600s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
                .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
                .config("spark.sql.warehouse.dir", f"file:///{spark_warehouse}") \
                .config("spark.local.dir", spark_temp) \
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
                .getOrCreate()

            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing Spark session: {str(e)}")
            raise

    def process_json_file(self, json_file_path, output_dir):
        """Process JSON file and extract hospital information and charges"""
        try:
            # Read JSON file
            df = self.spark.read \
                .option("multiLine", "true") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .json(json_file_path)

            # First, extract and flatten hospital information
            hospital_info_df = df.select(
                "hospital_name",
                # Convert arrays to strings by concatenating elements
                expr("concat_ws(', ', hospital_address)").alias("hospital_address"),
                expr("concat_ws(', ', hospital_location)").alias("hospital_location"),
                col("last_updated_on").cast("string"),
                "version"
            )

            # Explode the standard_charge_information array and extract nested fields
            charges_df = df.select(
                "hospital_name",
                explode("standard_charge_information").alias("charge_info")
            ).select(
                "hospital_name",
                "charge_info.description",
                explode("charge_info.code_information").alias("code_info"),
                explode("charge_info.standard_charges").alias("standard_charge_entry")
            ).select(
                "hospital_name",
                "description",
                "code_info.code",
                "code_info.type",
                explode(
                    when(col("standard_charge_entry.payers_information").isNull() | 
                         (size(col("standard_charge_entry.payers_information")) == 0),
                         array(
                             struct(
                                 lit("").alias("additional_payer_notes"),
                                 lit(0.0).cast("double").alias("estimated_amount"),
                                 lit("").alias("methodology"),
                                 lit("Self Pay").alias("payer_name"),
                                 lit("Cash Price").alias("plan_name"),
                                 lit("").alias("standard_charge_algorithm")
                             )
                         )
                    ).otherwise(col("standard_charge_entry.payers_information"))
                ).alias("payer_info"),
                coalesce(
                    col("standard_charge_entry.gross_charge").cast("decimal(18,2)"),
                    lit(0.00).cast("decimal(18,2)")
                ).alias("standard_charge_gross"),
                coalesce(
                    col("standard_charge_entry.discounted_cash").cast("decimal(18,2)"),
                    col("standard_charge_entry.gross_charge").cast("decimal(18,2)"),
                    lit(0.00).cast("decimal(18,2)")
                ).alias("standard_charge_negotiated_dollar"),
                when(
                    col("standard_charge_entry.minimum").isNotNull(),
                    col("standard_charge_entry.minimum").cast("decimal(18,2)")
                ).otherwise(lit("")).alias("standard_charge_min"),
                when(
                    col("standard_charge_entry.maximum").isNotNull(),
                    col("standard_charge_entry.maximum").cast("decimal(18,2)")
                ).otherwise(lit("")).alias("standard_charge_max")
            ).select(
                "hospital_name",
                "description",
                "code",
                "type",
                coalesce(col("payer_info.payer_name"), lit("Self Pay")).alias("payer_name"),
                coalesce(col("payer_info.plan_name"), lit("Cash Price")).alias("plan_name"),
                "standard_charge_gross",
                "standard_charge_negotiated_dollar",
                "standard_charge_min",
                "standard_charge_max",
                lit(True).alias("is_active"),
                current_timestamp().cast("timestamp").alias("created_at"),
                current_timestamp().cast("timestamp").alias("updated_at")
            )

            # Get original filename without extension
            original_filename = os.path.splitext(os.path.basename(json_file_path))[0]
            
            # Write hospital info to CSV directly in uploads folder
            hospital_info_path = os.path.join(output_dir, f"{original_filename}_address.csv")
            
            # Collect hospital info data first
            try:
                hospital_info_data = hospital_info_df.collect()
                hospital_info_columns = hospital_info_df.columns
                hospital_info_pdf = pd.DataFrame(hospital_info_data, columns=hospital_info_columns)
                hospital_info_pdf.to_csv(hospital_info_path, index=False, encoding='utf-8')
                
                # Get first hospital record for session data
                first_hospital = hospital_info_pdf.iloc[0].to_dict() if not hospital_info_pdf.empty else {}
            except Exception as e:
                logger.error(f"Error collecting hospital info data: {str(e)}")
                raise

            # Write charges to CSV directly in uploads folder
            charges_path = os.path.join(output_dir, f"{original_filename}_charges.csv")
            
            try:
                # Write header first
                charges_header_df = charges_df.select(
                    "hospital_name",
                    "description",
                    "code",
                    "type",
                    "payer_name",
                    "plan_name",
                    "standard_charge_gross",
                    "standard_charge_negotiated_dollar",
                    "standard_charge_min",
                    "standard_charge_max",
                    "is_active",
                    col("created_at").cast("string"),
                    col("updated_at").cast("string")
                ).limit(0)
                
                header_pdf = charges_header_df.toPandas()
                header_pdf.to_csv(charges_path, index=False, encoding='utf-8')
                
                # Get total count for chunking
                total_rows = charges_df.count()
                
                # Process in chunks with proper window partitioning
                window = Window.partitionBy("hospital_name") \
                              .orderBy("code", "payer_name", "plan_name")
                
                charges_df_with_row_num = charges_df.withColumn("row_num", row_number().over(window))
                
                # Calculate optimal number of partitions
                num_partitions = max(1, min(200, total_rows // 50000))  # Cap at 200 partitions
                charges_df_with_row_num = charges_df_with_row_num.repartition(num_partitions)
                
                # Process in chunks
                chunk_size = 50000
                for chunk_start in range(0, total_rows, chunk_size):
                    chunk_end = chunk_start + chunk_size
                    
                    chunk_data = charges_df_with_row_num.filter(
                        (col("row_num") > chunk_start) & 
                        (col("row_num") <= chunk_end)
                    ).select(
                        "hospital_name",
                        "description",
                        "code",
                        "type",
                        "payer_name",
                        "plan_name",
                        "standard_charge_gross",
                        "standard_charge_negotiated_dollar",
                        "standard_charge_min",
                        "standard_charge_max",
                        "is_active",
                        col("created_at").cast("string"),
                        col("updated_at").cast("string")
                    ).collect()
                    
                    if chunk_data:
                        chunk_pdf = pd.DataFrame(chunk_data, columns=header_pdf.columns)
                        chunk_pdf.to_csv(charges_path, mode='a', header=False, index=False, encoding='utf-8')
                    
                    logger.info(f"Wrote {min(chunk_end, total_rows)}/{total_rows} rows")
                
            except Exception as e:
                logger.error(f"Error processing charges data: {str(e)}")
                raise

            return hospital_info_path, charges_path, first_hospital

        except Exception as e:
            logger.error(f"Error processing JSON file: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        finally:
            try:
                # Attempt to stop Spark session gracefully
                if self.spark:
                    self.spark.stop()
                    self.spark = None
                    logger.info("Spark session stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Spark session: {str(e)}")

    def stop_spark(self):
        """Stop Spark session"""
        try:
            if self.spark:
                self.spark.stop()
                self.spark = None
                logger.info("Spark session stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {str(e)}")
            raise

def main():
    """Main function to test the JSON processor"""
    processor = None
    try:
        processor = JSONProcessor()
        
        # Example usage
        json_file = "path/to/your/input.json"
        output_dir = "path/to/output/directory"
        
        hospital_info_csv, charges_csv, first_hospital = processor.process_json_file(json_file, output_dir)
        print(f"Generated files:\nHospital Info: {hospital_info_csv}\nCharges: {charges_csv}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if processor:
            processor.stop_spark()

if __name__ == "__main__":
    main() 