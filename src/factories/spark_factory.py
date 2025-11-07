import logging
from typing import List, Dict, Optional
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class SparkFactory:
    @staticmethod
    def create_local_session(
        app_name: str = "LocalSparkApp",
        master: str = "local[*]",
        config: Optional[Dict[str, str]] = None
    ) -> SparkSession:
        logger.info(f" Creating local Spark session with app name: {app_name} and master: {master}")
        builder = SparkSession.builder.appName(app_name).master(master)

        # Default configuration for local execution
        default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "4",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
        # Merge with user config
        all_config = {**default_config, **(config or {})}

        for key, value in all_config.items():
            builder = builder.config(key, value)
        
        session = builder.getOrCreate()
        logger.info(f"Spark session created: {session.version}")
        
        return session
    
    @staticmethod
    def create_cluster_session(
        app_name: str,
        master: str,
        config: Optional[Dict[str, str]] = None
    ) -> SparkSession:
        logger.info(f"creating cluster Spark session with app name: {app_name} and master: {master} ")
        
        builder =  SparkSession.builder.appName(app_name).master(master)
        default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.dynamicAllocation.enabled": "true",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
        all_config = {**default_config, **(config or {})}

        for key, value in all_config.items():
            builder = builder.config(key, value)
        session = builder.getOrCreate()
        logger.info(f"Spark session created: {session.version}")
        return session
    
    @staticmethod
    def create_test_session(
        app_name: str = "Test"
    ) -> SparkSession:
        logger.info(f"Creating test Spark session with app name: {app_name}")

        return SparkSession.builder \
            .appName(app_name) \
            .master("local[1]") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.default.parallelism", "1") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

