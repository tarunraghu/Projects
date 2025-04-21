from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

class BaseIngestionStrategy(ABC):
    def __init__(self):
        self.spark = self._initialize_spark()
    
    def _initialize_spark(self):
        """Initialize Spark session with PostgreSQL driver"""
        try:
            return SparkSession.builder \
                .appName("Healthcare Data Processing") \
                .config("spark.jars", "postgresql-42.7.2.jar") \
                .getOrCreate()
        except Exception as e:
            logger.error(f"Error initializing Spark: {str(e)}")
            raise
    
    @abstractmethod
    def process_csv(self, file_path):
        """Process CSV file and return address and hospital dataframes"""
        pass
    
    @abstractmethod
    def read_json(self, file_path):
        """Read JSON file and return address and hospital dataframes"""
        pass
    
    def write_to_postgres(self, df, table_name):
        """Write DataFrame to PostgreSQL table"""
        try:
            # PostgreSQL connection properties
            postgres_properties = {
                "driver": "org.postgresql.Driver",
                "url": "jdbc:postgresql://localhost:5432/healthcarepoc",
                "user": "postgres",
                "password": "Consis10C!",
                "dbtable": table_name
            }
            
            # Write DataFrame to PostgreSQL
            df.write \
                .format("jdbc") \
                .options(**postgres_properties) \
                .mode("overwrite") \
                .save()
            
            logger.info(f"Successfully wrote data to {table_name}")
            
        except Exception as e:
            logger.error(f"Error writing to PostgreSQL: {str(e)}")
            raise 