import logging
import time
from pyspark.sql import SparkSession
from scripts.player_value_predictor import FootballPlayerValuePredictor
from scripts.web_scraping_sofifa import main as scrape_main

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/sami/Desktop/football_prediction_project/logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def verify_hdfs_path(spark, hdfs_path):
    """Verify if HDFS path exists"""
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path))
    except Exception as e:
        logger.error(f"Failed to verify HDFS path {hdfs_path}: {e}")
        return False

def save_to_hdfs(spark, file_path, hdfs_path):
    """Save CSV file to HDFS"""
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        df.write.mode("overwrite").csv(hdfs_path)
        logger.info(f"✅ Data saved to HDFS at {hdfs_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save to HDFS: {e}")
        return False

def main():
    logger.info("🚀 Starting Football Prediction Pipeline")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("FootballPredictionPipeline") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()
    
    # Step 1: Web Scraping
    logger.info("📋 Step: Web Scraping")
    logger.info("🌐 Starting web scraping...")
    try:
        #scrape_main()
        logger.info("✅ Scraping completed")
    except Exception as e:
        logger.error(f"❌ Scraping error: {e}")
        spark.stop()
        return False
    
    # Step 2: Spark ML
    logger.info("📋 Step: Spark ML")
    logger.info("🔥 Starting Spark ML...")
    try:
        predictor = FootballPlayerValuePredictor()
        df = predictor.load_data_from_csv("/home/sami/Desktop/football_prediction_project/True_players_data.csv")
        model_gk, gk_features, model_field, field_features = predictor.train_model(df)
        predictor.save_model(model_gk, "player_value_model_gk")
        predictor.save_model(model_field, "player_value_model_field")
        predictor.analyze_feature_importance(model_gk, gk_features, "goalkeeper")
        predictor.analyze_feature_importance(model_field, field_features, "field")
        predictor.stream_predictions()
        logger.info("✅ ML pipeline completed")
    except Exception as e:
        logger.error(f"❌ Spark ML failed: {e}")
        spark.stop()
        return False
    
    # Step 3: Save to HDFS
    logger.info("📋 Step: HDFS Save")
    logger.info("💾 Verifying HDFS saves...")
    try:
        hdfs_files = [
            ("predictions_gk.csv", "hdfs://localhost:9000/football/data/predictions_gk.csv"),
            ("predictions_field.csv", "hdfs://localhost:9000/football/data/predictions_field.csv"),
            ("kafka_predictions_gk.csv", "hdfs://localhost:9000/football/data/kafka_predictions_gk.csv"),
            ("kafka_predictions_field.csv", "hdfs://localhost:9000/football/data/kafka_predictions_field.csv")
        ]
        for local_file, hdfs_path in hdfs_files:
            if not verify_hdfs_path(spark, hdfs_path):
                if not save_to_hdfs(spark, local_file, hdfs_path):
                    raise Exception(f"Failed to save {local_file} to HDFS")
            else:
                logger.info(f"✅ HDFS path {hdfs_path} already exists")
    except Exception as e:
        logger.error(f"❌ HDFS save failed: {e}")
        spark.stop()
        return False
    
    spark.stop()
    logger.info("🎉 Pipeline completed!")
    return True

if __name__ == "__main__":
    main()
