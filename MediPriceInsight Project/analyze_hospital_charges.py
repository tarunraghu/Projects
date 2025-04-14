from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import traceback
import os

def process_hospital_data(spark, file_path, hospital_name):
    """Process hospital data and return dataframes"""
    print(f"\nProcessing data for {hospital_name}...")
    
    # Read the JSON file with optimized settings
    df = spark.read.option("multiLine", "true") \
                   .option("mode", "PERMISSIVE") \
                   .option("columnNameOfCorruptRecord", "_corrupt_record") \
                   .json(file_path)
    
    # Create hospital information dataframe
    hospital_info_df = df.select(
        col("hospital_name"),
        col("last_updated_on"),
        col("version"),
        array_join("hospital_location", ", ").alias("hospital_location"),
        array_join("hospital_address", ", ").alias("hospital_address")
    ).cache()

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
        col("charges.gross_charge").cast("decimal(20,2)").alias("standard_charge_gross"),
        col("charges.discounted_cash").cast("decimal(20,2)").alias("standard_charge_negotiated_dollar"),
        col("charges.minimum").cast("decimal(20,2)").alias("standard_charge_min"),
        col("charges.maximum").cast("decimal(20,2)").alias("standard_charge_max")
    ).repartition(10).cache()  # Repartition and cache for better memory management

    return hospital_info_df, final_charges_df

def save_dataframe_in_chunks(df, output_path, chunk_size=100000):
    """Save large dataframe to CSV in chunks"""
    print(f"Saving data to {output_path} in chunks...")
    
    # Get total count
    total_rows = df.count()
    print(f"Total rows to process: {total_rows}")
    
    # Process in chunks
    offset = 0
    while offset < total_rows:
        end = offset + chunk_size
        if end > total_rows:
            end = total_rows
            
        chunk_df = df.offset(offset).limit(chunk_size).toPandas()
        
        # For first chunk, write with header
        if offset == 0:
            chunk_df.to_csv(output_path, index=False, mode='w')
        else:
            # Append without header for subsequent chunks
            chunk_df.to_csv(output_path, index=False, mode='a', header=False)
        
        print(f"Processed {end} of {total_rows} rows")
        offset = end

def main():
    try:
        # Initialize Spark session with memory configuration and Windows settings
        print("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName("HospitalChargesAnalysis") \
            .config("spark.driver.memory", "12g") \
            .config("spark.executor.memory", "12g") \
            .config("spark.driver.maxResultSize", "4g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "4g") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("ERROR")

        # Define file paths and hospital names
        files = [
            ("752451969_kindred-hospital-tarrant-county---arlington_standardcharges (1).json", "Kindred Hospital"),
            ("62-1682205_MEDICAL-CITY-NORTH-HILLS_standardcharges.json", "Medical City North Hills")
        ]

        # Create output directory if it doesn't exist
        output_dir = "output"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Process each file
        for file_path, hospital_name in files:
            print(f"\n=== Processing {hospital_name} ===")
            
            # Process the data
            hospital_info_df, final_charges_df = process_hospital_data(spark, file_path, hospital_name)
            
            # Create hospital-specific output directory
            hospital_output_dir = os.path.join(output_dir, hospital_name.replace(" ", "_"))
            if not os.path.exists(hospital_output_dir):
                os.makedirs(hospital_output_dir)
            
            # Save the dataframes to CSV files
            print(f"\nSaving files for {hospital_name}...")
            
            # Save hospital information (small file, save directly)
            hospital_info_path = os.path.join(hospital_output_dir, "hospital_information.csv")
            hospital_info_df.toPandas().to_csv(hospital_info_path, index=False)
            print(f"Saved hospital information to {hospital_info_path}")
            
            # Save charges information in chunks
            charges_path = os.path.join(hospital_output_dir, "hospital_charges.csv")
            save_dataframe_in_chunks(final_charges_df, charges_path)
            
            # Show sample data
            print(f"\nSample hospital information for {hospital_name}:")
            hospital_info_df.limit(2).show(truncate=False)
            
            print(f"\nSample hospital charges for {hospital_name}:")
            final_charges_df.limit(2).show(truncate=False)
            
            # Clear cache to free up memory
            hospital_info_df.unpersist()
            final_charges_df.unpersist()

        print("\nAll files have been processed successfully!")
        print("Data has been saved in the 'output' directory with separate folders for each hospital.")

    except Exception as e:
        print("\nError occurred:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print("\nStack trace:")
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Stop the Spark session
        if 'spark' in locals():
            print("\nStopping Spark session...")
            spark.stop()

if __name__ == "__main__":
    main() 