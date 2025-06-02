from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, log1p, pow, expm1, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import logging
import json
import pandas as pd
import os
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from kafka.errors import KafkaError
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/sami/Desktop/football_prediction_project/logs/predictor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FootballPlayerValuePredictor:
    def __init__(self):
        """Initialize Spark session"""
        self.spark = SparkSession.builder \
            .appName("FootballPlayerValuePrediction") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        self.model_gk = None
        self.model_field = None
        self.pipeline_gk = None
        self.pipeline_field = None

    def load_data_from_csv(self, csv_path):
        """Load data from local CSV"""
        if not os.path.exists(csv_path):
            logger.error(f"Fichier {csv_path} non trouvé")
            raise FileNotFoundError(f"Fichier {csv_path} non trouvé")
        
        schema = StructType([
            StructField("Name", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Overall rating", IntegerType(), True),
            StructField("Potential", IntegerType(), True),
            StructField("Team", StringType(), True),
            StructField("ID", StringType(), True),
            StructField("Value", FloatType(), True),
            StructField("Wage", FloatType(), True),
            StructField("Total attacking", IntegerType(), True),
            StructField("Total skill", IntegerType(), True),
            StructField("Total movement", IntegerType(), True),
            StructField("Total power", IntegerType(), True),
            StructField("Total mentality", IntegerType(), True),
            StructField("Total defending", IntegerType(), True),
            StructField("Total goalkeeping", IntegerType(), True),
            StructField("Pace / Diving", IntegerType(), True),
            StructField("Shooting / Handling", IntegerType(), True),
            StructField("Passing / Kicking", IntegerType(), True),
            StructField("Dribbling / Reflexes", IntegerType(), True),
            StructField("Defending / Pace", IntegerType(), True),
            StructField("Position category", StringType(), True)
        ])
        full_path = f"file://{os.path.abspath(csv_path)}"
        df = self.spark.read.csv(full_path, schema=schema, header=True).repartition(8)
        count = df.count()
        logger.info(f"Chargé {count} lignes depuis {csv_path}")
        logger.info(f"Schéma: {df.schema}")
        # Validate data
        if count == 0:
            logger.error("Aucune donnée chargée depuis le CSV")
            raise ValueError("Aucune donnée chargée depuis le CSV")
        # Check for null values
        df.select([col(c).isNull().cast("int").alias(c) for c in df.columns]).groupBy().sum().show()
        # Summarize Value
        df.select("Value").describe().show()
        # Sum numeric columns with validation
        numeric_cols = ["Age", "Overall rating", "Potential", "Value", "Wage", "Total attacking", 
                        "Total skill", "Total movement", "Total power", "Total mentality", 
                        "Total defending", "Total goalkeeping", "Pace / Diving", 
                        "Shooting / Handling", "Passing / Kicking", "Dribbling / Reflexes", 
                        "Defending / Pace"]
        sums_df = df.select([sum(col(c)).alias(f"sum({c})") for c in numeric_cols])
        sums_row = sums_df.collect()[0]
        for col_name in numeric_cols:
            sum_value = sums_row[f"sum({col_name})"]
            if sum_value is None or sum_value == 0:
                logger.warning(f"Somme nulle ou None pour {col_name}")
        sums_df.show()
        return df

    def load_data_from_kafka(self, topic='players-data', bootstrap_servers='127.0.0.1:9092', retries=5):
        """Load data from Kafka with retries"""
        attempt = 0
        while attempt < retries:
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=bootstrap_servers,
                    auto_offset_reset='earliest',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=20000
                )
                logger.info(f"Connecté à Kafka: {bootstrap_servers}, topic: {topic}")
                partitions = consumer.partitions_for_topic(topic)
                if partitions:
                    for p in partitions:
                        tp = TopicPartition(topic, p)
                        consumer.seek_to_end(tp)
                        end_offset = consumer.position(tp)
                        consumer.seek_to_beginning(tp)
                        start_offset = consumer.position(tp)
                        logger.info(f"Partition {p}: start_offset={start_offset}, end_offset={end_offset}")
                data = []
                for message in consumer:
                    logger.info(f"Message reçu: {message.value}")
                    data.append(message.value)
                    if len(data) >= 300:
                        break
                consumer.close()
                if not data:
                    logger.warning("Aucune donnée reçue depuis Kafka")
                    return None
                logger.info(f"Reçu {len(data)} joueurs depuis Kafka")
                pdf = pd.DataFrame(data)
                # Define Kafka schema with IntegerType
                schema = StructType([
                    StructField("Name", StringType(), True),
                    StructField("ID", StringType(), True),
                    StructField("Age", IntegerType(), True),
                    StructField("Overall rating", IntegerType(), True),
                    StructField("Potential", IntegerType(), True),
                    StructField("Team", StringType(), True),
                    StructField("Position category", StringType(), True),
                    StructField("Value", FloatType(), True),
                    StructField("Wage", FloatType(), True),
                    StructField("Total attacking", IntegerType(), True),
                    StructField("Total skill", IntegerType(), True),
                    StructField("Total movement", IntegerType(), True),
                    StructField("Total power", IntegerType(), True),
                    StructField("Total mentality", IntegerType(), True),
                    StructField("Total defending", IntegerType(), True),
                    StructField("Total goalkeeping", IntegerType(), True),
                    StructField("Pace / Diving", IntegerType(), True),
                    StructField("Shooting / Handling", IntegerType(), True),
                    StructField("Passing / Kicking", IntegerType(), True),
                    StructField("Dribbling / Reflexes", IntegerType(), True),
                    StructField("Defending / Pace", IntegerType(), True)
                ])
                df = self.spark.createDataFrame(pdf, schema=schema).repartition(8)
                count = df.count()
                logger.info(f"Chargé {count} lignes depuis Kafka")
                logger.info(f"Schéma Kafka: {df.schema}")
                # Log Kafka data stats
                df.select("Value", "Overall rating", "Potential").describe().show()
                # Add Value_log and filter outliers (±3.0 stddev)
                df = df.withColumn("Value_log", log1p(col("Value")))
                value_stats = df.select("Value_log").describe().collect()
                value_mean = float(value_stats[1]["Value_log"])
                value_std = float(value_stats[2]["Value_log"])
                df = df.filter(col("Value_log").between(value_mean - 3.0 * value_std, value_mean + 3.0 * value_std))
                return df
            except KafkaError as e:
                attempt += 1
                logger.error(f"Erreur Kafka (tentative {attempt}/{retries}): {e}")
                if attempt == retries:
                    logger.error("Échec après toutes les tentatives")
                    return None
            except Exception as e:
                logger.error(f"Erreur inattendue: {e}")
                return None

    def preprocess_data(self, df, is_goalkeeper=False):
        """Preprocess data for ML"""
        df = df.withColumn("Value", when(col("Value") > 1.7e8, 1.7e8).otherwise(col("Value")))
        df = df.withColumn("Value_log", log1p(col("Value")))
        # Filter outliers (±2.5 stddev for field players, ±3.0 for goalkeepers)
        value_stats = df.select("Value_log").describe().collect()
        value_mean = float(value_stats[1]["Value_log"])
        value_std = float(value_stats[2]["Value_log"])
        stddev_limit = 3.0 if is_goalkeeper else 2.5
        df = df.filter(col("Value_log").between(value_mean - stddev_limit * value_std, value_mean + stddev_limit * value_std))
        
        shared_features = ["Age", "Overall rating", "Potential", "Wage"]
        gk_features = [
            "Total goalkeeping", "Pace / Diving", "Shooting / Handling",
            "Passing / Kicking", "Dribbling / Reflexes"
        ]
        field_features = [
            "Total attacking", "Total skill", "Total movement", "Total power",
            "Total mentality", "Total defending"
        ]
        
        feature_columns = shared_features + (gk_features if is_goalkeeper else field_features)
        logger.info(f"Features utilisées ({'gardiens' if is_goalkeeper else 'joueurs de champ'}): {feature_columns}")
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        df = assembler.transform(df)
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(df)
        df = scaler_model.transform(df)
        # Log scaler mean and stddev
        logger.info(f"Scaler mean: {scaler_model.mean.toArray()}")
        logger.info(f"Scaler stddev: {scaler_model.std.toArray()}")
        return df, feature_columns, scaler_model

    def create_ml_pipeline(self):
        """Create ML pipeline"""
        rf = RandomForestRegressor(featuresCol="scaled_features", labelCol="Value_log")
        return Pipeline(stages=[rf])

    def train_model(self, df):
        """Train models for goalkeepers and field players"""
        start_time = time.time()
        df = df.withColumn("Position category", lower(col("Position category")))
        df = df.na.fill({"Position category": "unknown"})
        df = df.withColumn(
            "Position category indexed",
            when(col("Position category") == "attaquant", 0.0)
            .when(col("Position category") == "milieu", 1.0)
            .when(col("Position category") == "défenseur", 2.0)
            .when(col("Position category") == "gardien", 3.0)
            .otherwise(4.0)
        )
        df_gk = df.filter(col("Position category indexed") == 3.0)
        df_field = df.filter(col("Position category indexed").isin([0.0, 1.0, 2.0]))
        logger.info(f"Taille des données gardiens: {df_gk.count()}")
        logger.info(f"Taille des données joueurs de champ: {df_field.count()}")
        
        df_gk, gk_features, gk_scaler = self.preprocess_data(df_gk, is_goalkeeper=True)
        df_field, field_features, field_scaler = self.preprocess_data(df_field, is_goalkeeper=False)
        
        self.pipeline_gk = self.create_ml_pipeline()
        self.pipeline_field = self.create_ml_pipeline()
        
        param_grid_gk = ParamGridBuilder() \
            .addGrid(self.pipeline_gk.getStages()[-1].numTrees, [150, 200]) \
            .addGrid(self.pipeline_gk.getStages()[-1].maxDepth, [7]) \
            .addGrid(self.pipeline_gk.getStages()[-1].subsamplingRate, [0.9]) \
            .addGrid(self.pipeline_gk.getStages()[-1].minInstancesPerNode, [5]) \
            .build()
        
        param_grid_field = ParamGridBuilder() \
            .addGrid(self.pipeline_field.getStages()[-1].numTrees, [50]) \
            .addGrid(self.pipeline_field.getStages()[-1].maxDepth, [10]) \
            .build()
        
        evaluator = RegressionEvaluator(labelCol="Value_log", predictionCol="prediction", metricName="rmse")
        
        if df_gk.count() > 0:
            train_gk, test_gk = df_gk.randomSplit([0.8, 0.2], seed=42)
            cv_gk = CrossValidator(
                estimator=self.pipeline_gk,
                estimatorParamMaps=param_grid_gk,
                evaluator=evaluator,
                numFolds=3
            )
            self.model_gk = cv_gk.fit(train_gk)
            predictions_gk = self.model_gk.transform(test_gk)
            predictions_gk = predictions_gk.withColumn("Value_euros", expm1(col("Value_log")))
            predictions_gk = predictions_gk.withColumn("Prediction_euros", expm1(col("prediction")))
            rmse_gk = evaluator.evaluate(predictions_gk)
            logger.info(f"RMSE gardiens: {rmse_gk}")
            best_params_gk = self.model_gk.bestModel.stages[-1].extractParamMap()
            logger.info(f"Meilleurs paramètres gardiens: {best_params_gk}")
            predictions_gk.withColumn("error", pow(col("Value_log") - col("prediction"), 2)) \
                .select("error").describe().show()
            predictions_gk.select("Name", "Value_log", "Value_euros", "prediction", "Prediction_euros").show(5)
            predictions_gk.select("Name", "Value_euros", "Prediction_euros").coalesce(1).write.csv(
                "hdfs://localhost:9000/football/data/predictions_gk.csv", header=True, mode="overwrite")
            predictions_gk.select("Name", "Value_euros", "Prediction_euros").coalesce(1).write.csv(
                "predictions_gk.csv", header=True, mode="overwrite")
            logger.info("Prédictions gardiens sauvegardées dans predictions_gk.csv")
        else:
            logger.warning("Aucune donnée pour les gardiens")
            self.model_gk = None
        
        train_field, test_field = df_field.randomSplit([0.8, 0.2], seed=42)
        cv_field = CrossValidator(
            estimator=self.pipeline_field,
            estimatorParamMaps=param_grid_field,
            evaluator=evaluator,
            numFolds=2
        )
        self.model_field = cv_field.fit(train_field)
        predictions_field = self.model_field.transform(test_field)
        predictions_field = predictions_field.withColumn("Value_euros", expm1(col("Value_log")))
        predictions_field = predictions_field.withColumn("Prediction_euros", expm1(col("prediction")))
        rmse_field = evaluator.evaluate(predictions_field)
        logger.info(f"RMSE joueurs de champ: {rmse_field}")
        best_params_field = self.model_field.bestModel.stages[-1].extractParamMap()
        logger.info(f"Meilleurs paramètres joueurs de champ: {best_params_field}")
        predictions_field.withColumn("error", pow(col("Value_log") - col("prediction"), 2)) \
            .select("error").describe().show()
        predictions_field.select("Name", "Value_log", "Value_euros", "prediction", "Prediction_euros").show(5)
        predictions_field.select("Name", "Value_euros", "Prediction_euros").coalesce(1).write.csv(
            "hdfs://localhost:9000/football/data/predictions_field.csv", header=True, mode="overwrite")
        predictions_field.select("Name", "Value_euros", "Prediction_euros").coalesce(1).write.csv(
            "predictions_field.csv", header=True, mode="overwrite")
        logger.info("Prédictions joueurs de champ sauvegardées dans predictions_field.csv")
        
        logger.info(f"Temps d'entraînement: {time.time() - start_time:.2f} secondes")
        return self.model_gk, gk_features, self.model_field, field_features

    def save_model(self, model, path):
        """Save model"""
        if model is not None:
            model.write().overwrite().save(f"hdfs://localhost:9000/football/models/{path}")
            logger.info(f"Modèle sauvegardé dans hdfs://localhost:9000/football/models/{path}")
        else:
            logger.warning(f"Aucun modèle à sauvegarder pour {path}")

    def load_model(self, path):
        """Load model"""
        from pyspark.ml.tuning import CrossValidatorModel
        return CrossValidatorModel.load(f"hdfs://localhost:9000/football/models/{path}")

    def predict_player_value(self, model, player_data, is_goalkeeper=False):
        """Predict player value"""
        if model is None:
            logger.warning("Aucun modèle disponible pour la prédiction")
            return None
        if isinstance(player_data, pd.DataFrame):
            player_df = self.spark.createDataFrame(player_data)
        else:
            player_df = player_data
        
        player_df, feature_columns, scaler_model = self.preprocess_data(player_df, is_goalkeeper)
        predictions = model.transform(player_df)
        predictions = predictions.withColumn("Value_euros", expm1(col("Value_log")))
        predictions = predictions.withColumn("Prediction_euros", expm1(col("prediction")))
        logger.info(f"Échantillon de prédictions ({'gardiens' if is_goalkeeper else 'joueurs de champ'}):")
        predictions.select("Name", "Value_euros", "Prediction_euros").show(5)
        return predictions

    def analyze_feature_importance(self, model, feature_names, model_name):
        """Analyze feature importance"""
        if model is None:
            logger.warning(f"Aucun modèle pour analyser l'importance des features ({model_name})")
            return []
        rf_model = model.bestModel.stages[-1]
        importances = rf_model.featureImportances.toArray()
        feature_importance = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)
        logger.info(f"Importance des features ({model_name}):")
        with open(f"feature_importance_{model_name}.txt", "w") as f:
            for feature, importance in feature_importance:
                logger.info(f"{feature}: {importance}")
                f.write(f"{feature}: {importance}\n")
        return feature_importance

    def stream_predictions(self):
        """Stream predictions from Kafka"""
        df = self.load_data_from_kafka()
        if df:
            df = df.withColumn("Position category", lower(col("Position category")))
            df = df.na.fill({"Position category": "unknown"})
            df = df.withColumn(
                "Position category indexed",
                when(col("Position category") == "attaquant", 0.0)
                .when(col("Position category") == "milieu", 1.0)
                .when(col("Position category") == "défenseur", 2.0)
                .when(col("Position category") == "gardien", 3.0)
                .otherwise(4.0)
            )
            df_gk = df.filter(col("Position category indexed") == 3.0)
            df_field = df.filter(col("Position category indexed").isin([0.0, 1.0, 2.0]))
            logger.info(f"Données Kafka gardiens: {df_gk.count()}")
            logger.info(f"Données Kafka joueurs de champ: {df_field.count()}")
            if self.model_gk and df_gk.count() > 0:
                predictions_gk = self.predict_player_value(self.model_gk, df_gk, is_goalkeeper=True)
                if predictions_gk:
                    predictions_gk.select("Name", "Value_euros", "Prediction_euros").coalesce(1).write.csv(
                        "hdfs://localhost:9000/football/data/kafka_predictions_gk.csv", header=True, mode="overwrite")
                    predictions_gk.select("Name", "Value_euros", "Prediction_euros").coalesce(1).write.csv(
                        "kafka_predictions_gk.csv", header=True, mode="overwrite")
                    logger.info("Prédictions Kafka gardiens sauvegardées dans kafka_predictions_gk.csv")
            if self.model_field and df_field.count() > 0:
                predictions_field = self.predict_player_value(self.model_field, df_field, is_goalkeeper=False)
                if predictions_field:
                    predictions_field.select("Name", "Value_euros", "Prediction_euros").coalesce(1).write.csv(
                        "hdfs://localhost:9000/football/data/kafka_predictions_field.csv", header=True, mode="overwrite")
                    predictions_field.select("Name", "Value_euros", "Prediction_euros").coalesce(1).write.csv(
                        "kafka_predictions_field.csv", header=True, mode="overwrite")
                    logger.info("Prédictions Kafka joueurs de champ sauvegardées dans kafka_predictions_field.csv")
        else:
            logger.warning("Aucune donnée à prédire en streaming")

def main():
    predictor = FootballPlayerValuePredictor()
    
    csv_path = "/home/sami/Desktop/football_prediction_project/True_players_data.csv"
    try:
        df = predictor.load_data_from_csv(csv_path)
    except Exception as e:
        logger.error(f"Échec du chargement des données: {e}")
        raise
    
    try:
        model_gk, gk_features, model_field, field_features = predictor.train_model(df)
    except Exception as e:
        logger.error(f"Échec de l'entraînement: {e}")
        raise
    
    try:
        predictor.save_model(model_gk, "player_value_model_gk")
        predictor.save_model(model_field, "player_value_model_field")
    except Exception as e:
        logger.error(f"Échec de la sauvegarde des modèles: {e}")
        raise
    
    try:
        if model_gk:
            predictor.analyze_feature_importance(model_gk, gk_features, "goalkeeper")
        if model_field:
            predictor.analyze_feature_importance(model_field, field_features, "field")
    except Exception as e:
        logger.error(f"Échec de l'analyse des features: {e}")
        raise
    
    try:
        predictor.stream_predictions()
    except Exception as e:
        logger.error(f"Échec des prédictions en streaming: {e}")
        raise
    
    predictor.spark.stop()

if __name__ == "__main__":
    main()
