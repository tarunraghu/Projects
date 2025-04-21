from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, regexp_extract, when, lit, count, trim, upper, struct, array, explode, round
from .base_strategy import BaseIngestionStrategy
import logging
import re
import traceback

logger = logging.getLogger(__name__)

class Type2SparkIngestionStrategy(BaseIngestionStrategy):
    def _initialize_spark(self):
        """Initialize Spark session with Type 2 specific configurations"""
        if not hasattr(self, 'spark') or self.spark._jsc.sc().isStopped():
            self.spark = SparkSession.builder \
                .appName("Type2Ingestion") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .config("spark.sql.shuffle.partitions", "10") \
                .config("spark.sql.analyzer.maxIterations", "200") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
        return self.spark

    def process_csv(self, file_path):
        """Process Type 2 CSV file with detailed logging"""
        try:
            logger.info(f"Starting to process CSV file: {file_path}")
            
            # Initialize Spark with Type 2 specific configs
            self._initialize_spark()
            
            # Read CSV if file_path is a string, otherwise assume it's already a DataFrame
            if isinstance(file_path, str):
                logger.info("Reading CSV file")
                df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("mode", "PERMISSIVE") \
                    .option("columnNameOfCorruptRecord", "_corrupt_record") \
                    .csv(file_path)
            else:
                logger.info("Using provided DataFrame")
                df = file_path
            
            if not self.validate_dataframe(df, "Initial CSV Load"):
                raise ValueError("Failed to load initial CSV properly")

            # Cache the dataframe
            df.cache()
            df.count()  # Force cache evaluation
            
            # Log column information
            logger.info(f"Available columns: {df.columns}")
            
            # Find all code type columns that match the pattern code|x|type
            code_type_columns = sorted([col_name for col_name in df.columns if '|type' in col_name.lower()])
            logger.info(f"Found code type columns: {code_type_columns}")
            
            # Initialize variables for code and code_type
            code_type_col = None
            code_col = None
            
            # Try to find CPT values in each code type column
            for type_col in code_type_columns:
                # Get total count and CPT count for the column
                type_stats = df.agg(
                    count(col(type_col)).alias("total_count"),
                    count(when(trim(upper(col(type_col))).like("%CPT%"), True)).alias("cpt_count")
                ).collect()[0]
                
                total_count = type_stats["total_count"]
                cpt_count = type_stats["cpt_count"]
                
                logger.info(f"Column {type_col} - Total rows: {total_count}, CPT rows: {cpt_count}")
                
                if cpt_count > 0:
                    code_type_col = type_col
                    # Extract the number from the type column (e.g., 'code|3|type' -> '3')
                    col_num = type_col.split('|')[1]
                    # Construct the corresponding code column name
                    potential_code_col = f"code|{col_num}"
                    
                    if potential_code_col in df.columns:
                        code_col = potential_code_col
                        logger.info(f"Found matching code column: {code_col} for type column: {code_type_col}")
                        break
            
            if not code_type_col or not code_col:
                error_msg = "No code column with CPT type found in any of the code type columns"
                logger.error(error_msg)
                logger.error("Available columns: " + ", ".join(df.columns))
                raise ValueError(error_msg)
            
            # Find standard charge columns
            standard_charge_columns = {
                'gross': None,
                'min': None,
                'max': None
            }
            
            # Find all negotiated dollar columns and other related columns
            negotiated_dollar_columns = []
            negotiated_algorithm_columns = []
            estimated_amount_columns = []
            
            # Look for standard charge columns with different patterns
            for col_name in df.columns:
                col_lower = col_name.lower()
                if ('standard' in col_lower and 'charge' in col_lower) or ('gross' in col_lower and 'charge' in col_lower):
                    if 'gross' in col_lower:
                        standard_charge_columns['gross'] = col_name
                    elif 'negotiated_dollar' in col_lower:
                        negotiated_dollar_columns.append(col_name)
                    elif 'negotiated_algorithm' in col_lower:
                        negotiated_algorithm_columns.append(col_name)
                    elif 'min' in col_lower:
                        standard_charge_columns['min'] = col_name
                    elif 'max' in col_lower:
                        standard_charge_columns['max'] = col_name
                elif 'estimated_amount' in col_lower:
                    estimated_amount_columns.append(col_name)
            
            logger.info(f"Found standard charge columns: {standard_charge_columns}")
            logger.info(f"Found negotiated dollar columns: {negotiated_dollar_columns}")
            
            # Create a new DataFrame with only CPT rows
            df_cpt = df.filter(trim(upper(col(code_type_col))).like("%CPT%"))
            
            # Log the count of filtered rows
            cpt_count = df_cpt.count()
            logger.info(f"Found {cpt_count} rows with CPT code type")
            
            if cpt_count == 0:
                error_msg = "No rows found with CPT code type"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # First select all necessary columns
            df_with_columns = df_cpt.select(
                'description',
                code_col,
                code_type_col,
                standard_charge_columns['gross'],
                standard_charge_columns['min'],
                standard_charge_columns['max'],
                *negotiated_dollar_columns,  # Include all negotiated dollar columns
                *negotiated_algorithm_columns,  # Include all negotiated algorithm columns
                *estimated_amount_columns  # Include all estimated amount columns
            )

            # Create structs for negotiated dollar columns
            negotiated_structs = []
            for col_name in negotiated_dollar_columns:
                parts = col_name.split('|')
                if len(parts) >= 4:  # standard_charge|payer|plan|negotiated_dollar
                    payer = parts[1]
                    plan = parts[2]
                    
                    # Find corresponding columns for this payer/plan combination
                    base_payer_plan = f"{parts[0]}|{payer}|{plan}"
                    
                    # Get corresponding columns
                    negotiated_algorithm = next((c for c in negotiated_algorithm_columns if c.startswith(base_payer_plan)), None)
                    estimated_amount = next((c for c in estimated_amount_columns if c.startswith(base_payer_plan)), None)
                    
                    # Create struct with all related columns
                    negotiated_structs.append(
                        struct(
                            lit(payer).alias("payer_name"),
                            lit(plan).alias("plan_name"),
                            col(col_name).cast('decimal(20,2)').alias("standard_charge_negotiated_dollar"),
                            col(negotiated_algorithm).alias("standard_charge_negotiated_algorithm") if negotiated_algorithm else lit(None).alias("standard_charge_negotiated_algorithm"),
                            col(estimated_amount).cast('decimal(20,2)').alias("estimated_amount") if estimated_amount else lit(None).cast('decimal(20,2)').alias("estimated_amount")
                        )
                    )
            
            # Create the array of structs and explode
            if negotiated_structs:
                # Create the array of structs
                df_exploded = df_with_columns.withColumn(
                    "negotiated_info",
                    explode(array(*negotiated_structs))
                )
            else:
                # If no negotiated dollar columns found, create empty structs
                df_exploded = df_with_columns.withColumn("negotiated_info", 
                    explode(array(struct(
                        lit("").alias("payer_name"),
                        lit("").alias("plan_name"),
                        lit(0.0).cast('decimal(20,2)').alias("standard_charge_negotiated_dollar"),
                        lit(None).alias("standard_charge_negotiated_algorithm"),
                        lit(None).cast('decimal(20,2)').alias("estimated_amount")
                    ))))
            
            # Select final columns
            df_final = df_exploded.select(
                col("description"),
                col(code_col).alias("code"),
                col(code_type_col).alias("code_type"),
                col("negotiated_info.payer_name"),
                col("negotiated_info.plan_name"),
                col(standard_charge_columns['gross']).alias("standard_charge_gross"),
                col("negotiated_info.standard_charge_negotiated_dollar"),
                col(standard_charge_columns['min']).alias("standard_charge_min"),
                col(standard_charge_columns['max']).alias("standard_charge_max")
            )
            
            # Round numeric columns to 2 decimal places
            numeric_columns = [
                'standard_charge_gross', 'standard_charge_negotiated_dollar',
                'standard_charge_min', 'standard_charge_max'
            ]
            
            for numeric_col in numeric_columns:
                if numeric_col in df_final.columns:
                    df_final = df_final.withColumn(numeric_col, round(col(numeric_col).cast('decimal(20,2)'), 2))
            
            return df_final
            
        except Exception as e:
            logger.error(f"Error processing Type 2 CSV: {str(e)}")
            logger.error(f"Detailed traceback: {traceback.format_exc()}")
            raise

    def read_json(self, file_path):
        """Read JSON file and return address and hospital dataframes"""
        raise NotImplementedError("JSON processing not implemented for Type 2 strategy") 