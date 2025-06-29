import logging
import json
import pandas as pd
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when, log1p, pow, expm1, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, LinearRegression
from pyspark.ml.regression import RandomForestRegressionModel, LinearRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.classification import MultilayerPerceptronClassifier
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from kafka.errors import KafkaError
import threading

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
        self.spark = SparkSession.builder \
            .appName("FootballPlayerValuePrediction") \
            .config("spark.executor.memory", "6g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.cores", "2") \
            .config("spark.sql.shuffle.partitions", "16") \
            .config("spark.default.parallelism", "16") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.executor.memoryOverhead", "1g") \
            .config("spark.shuffle.service.enabled", "true") \
            .config("spark.broadcast.blockSize", "20m") \
            .getOrCreate()
        self.logger = logging.getLogger(__name__)
        self.model_rf_gk = None
        self.model_svm_gk = None
        self.model_xgb_gk = None
        self.model_dl_gk = None
        self.model_rf_field = None
        self.model_svm_field = None
        self.model_xgb_field = None
        self.model_dl_field = None
        self.gk_features = None
        self.field_features = None


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
        df = self.spark.read.csv(full_path, schema=schema, header=True).repartition(4)
        count = df.count()
        logger.info(f"Chargé {count} lignes depuis {csv_path}")
        logger.info(f"Schéma: {df.schema}")
        
        if count == 0:
            logger.error("Aucune donnée chargée depuis le CSV")
            raise ValueError("Aucune donnée chargée depuis le CSV")
        
        df.select([col(c).isNull().cast("int").alias(c) for c in df.columns]).groupBy().sum().show()
        df.select("Value").describe().show()
        
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
                
                df = self.spark.createDataFrame(pdf, schema=schema).repartition(4)
                count = df.count()
                logger.info(f"Chargé {count} lignes depuis Kafka")
                logger.info(f"Schéma Kafka: {df.schema}")
                
                df.select("Value", "Overall rating", "Potential").describe().show()
                
                df = df.withColumn("Value_log", log1p(col("Value")))
                value_stats = df.select("Value_log").describe().collect()
                value_mean = float(value_stats[1]["Value_log"])
                value_std = float(value_stats[2]["Value_log"])
                
                df = df.filter(col("Value_log").between(value_mean - 3.0 * value_std, value_mean + 3.0 * value_std))
                
                numeric_cols = ["Age", "Overall rating", "Potential", "Value", "Wage", "Total attacking", 
                              "Total skill", "Total movement", "Total power", "Total mentality", 
                              "Total defending", "Total goalkeeping", "Pace / Diving", 
                              "Shooting / Handling", "Passing / Kicking", "Dribbling / Reflexes", 
                              "Defending / Pace"]
                
                for col_name in numeric_cols:
                    df = df.withColumn(col_name, col(col_name).cast(IntegerType()).cast(FloatType()))
                
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

    def preprocess_data(self, df, is_goalkeeper=False, use_combined_scaler=False):
        """Preprocess data for ML"""
        df = df.withColumn("Value", when(col("Value") > 1.7e8, 1.7e8).otherwise(col("Value")))
        df = df.withColumn("Value_log", log1p(col("Value")))
        
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
        
        if "scaled_features" in df.columns:
            df = df.drop("scaled_features")
        
        if use_combined_scaler:
            scaler_model = self.combined_scaler_gk if is_goalkeeper else self.combined_scaler_field
            if scaler_model is None:
                logger.error("Combined scaler not available for Kafka data")
                raise ValueError("Combined scaler not available")
        else:
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            scaler_model = scaler.fit(df)
            if is_goalkeeper:
                self.scaler_gk = scaler_model
            else:
                self.scaler_field = scaler_model
        
        df = scaler_model.transform(df)
        
        logger.info(f"Scaler mean ({'gardiens' if is_goalkeeper else 'joueurs de champ'}): {scaler_model.mean.toArray()}")
        logger.info(f"Scaler stddev ({'gardiens' if is_goalkeeper else 'joueurs de champ'}): {scaler_model.std.toArray()}")
        
        return df, feature_columns, scaler_model

    def create_combined_scaler(self, training_df, kafka_df):
        """Create a combined scaler for training and Kafka data"""
        training_df = training_df.withColumn("Position category", lower(col("Position category")))
        training_df = training_df.na.fill({"Position category": "unknown"})
        training_df = training_df.withColumn("Value_log", log1p(col("Value")))
        training_df = training_df.withColumn(
            "Position category indexed",
            when(col("Position category") == "attaquant", 0.0)
            .when(col("Position category") == "milieu", 1.0)
            .when(col("Position category") == "défenseur", 2.0)
            .when(col("Position category") == "gardien", 3.0)
            .otherwise(4.0)
        )
        
        kafka_df = kafka_df.withColumn("Position category", lower(col("Position category")))
        kafka_df = kafka_df.na.fill({"Position category": "unknown"})
        kafka_df = kafka_df.withColumn(
            "Position category indexed",
            when(col("Position category") == "attaquant", 0.0)
            .when(col("Position category") == "milieu", 1.0)
            .when(col("Position category") == "défenseur", 2.0)
            .when(col("Position category") == "gardien", 3.0)
            .otherwise(4.0)
        )
        
        common_columns = ["Name", "ID", "Age", "Overall rating", "Potential", "Team", "Position category",
                         "Value", "Wage", "Total attacking", "Total skill", "Total movement", "Total power",
                         "Total mentality", "Total defending", "Total goalkeeping", "Pace / Diving",
                         "Shooting / Handling", "Passing / Kicking", "Dribbling / Reflexes", "Defending / Pace",
                         "Value_log", "Position category indexed"]
        
        training_df = training_df.select(*common_columns)
        kafka_df = kafka_df.select(*common_columns)
        
        # Goalkeepers
        gk_training = training_df.filter(col("Position category indexed") == 3.0)
        gk_kafka = kafka_df.filter(col("Position category indexed") == 3.0)
        gk_combined = gk_training.union(gk_kafka)
        gk_combined, gk_features, scaler_gk = self.preprocess_data(gk_combined, is_goalkeeper=True, use_combined_scaler=False)
        self.combined_scaler_gk = scaler_gk
        logger.info("Combined scaler for goalkeepers created")
        
        # Field players
        field_training = training_df.filter(col("Position category indexed").isin([0.0, 1.0, 2.0]))
        field_kafka = kafka_df.filter(col("Position category indexed").isin([0.0, 1.0, 2.0]))
        field_combined = field_training.union(field_kafka)
        field_combined, field_features, scaler_field = self.preprocess_data(field_combined, is_goalkeeper=False, use_combined_scaler=False)
        self.combined_scaler_field = scaler_field
        logger.info("Combined scaler for field players created")

    def create_ml_pipelines(self):
        """Create ML pipelines for all models"""
        # Random Forest
        rf = RandomForestRegressor(featuresCol="scaled_features", labelCol="Value_log",numTrees=20, maxDepth=7)
        pipeline_rf = Pipeline(stages=[rf])        
        # SVM (using Linear Regression as alternative)
        svm = LinearRegression(featuresCol="scaled_features", labelCol="Value_log")
        pipeline_svm = Pipeline(stages=[svm])        
        # XGBoost alternative (Random Forest with different parameters)
        xgb = RandomForestRegressor(
            featuresCol="scaled_features", 
            labelCol="Value_log",
            numTrees=100,
            maxDepth=6,
            subsamplingRate=0.8        
        )
        pipeline_xgb = Pipeline(stages=[xgb])        
        # Deep Learning (Linear Regression with regularization as DL alternative)
        dl = LinearRegression(
            featuresCol="scaled_features", 
            labelCol="Value_log",
            regParam=0.01,
            elasticNetParam=0.5
        )
        pipeline_dl = Pipeline(stages=[dl])        
        return pipeline_rf, pipeline_svm, pipeline_xgb, pipeline_dl
       
  
    def train_model(self, df):
        """Train all models for goalkeepers and field players"""
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
    
    # Create pipelines
        pipeline_rf, pipeline_svm, pipeline_xgb, pipeline_dl = self.create_ml_pipelines()
    
    # Parameter grids for different models
        param_grid_rf = ParamGridBuilder() \
            .addGrid(pipeline_rf.getStages()[-1].numTrees, [20, 50]) \
            .addGrid(pipeline_rf.getStages()[-1].maxDepth, [7, 10]) \
            .build()
    
        param_grid_svm = ParamGridBuilder() \
            .addGrid(pipeline_svm.getStages()[-1].regParam, [0.1, 1.0]) \
            .build()
    
        param_grid_xgb = ParamGridBuilder() \
            .addGrid(pipeline_xgb.getStages()[-1].numTrees, [80, 120]) \
            .addGrid(pipeline_xgb.getStages()[-1].maxDepth, [5, 8]) \
            .build()
    
        param_grid_dl = ParamGridBuilder() \
            .addGrid(pipeline_dl.getStages()[-1].regParam, [0.01, 0.1]) \
            .addGrid(pipeline_dl.getStages()[-1].elasticNetParam, [0.3, 0.7]) \
            .build()
    
        evaluator = RegressionEvaluator(labelCol="Value_log", predictionCol="prediction", metricName="rmse")
    
        models_info = {
            'RandomForest': (pipeline_rf, param_grid_rf),
            'SVM': (pipeline_svm, param_grid_svm),
            'XGBoost': (pipeline_xgb, param_grid_xgb),
            'DeepLearning': (pipeline_dl, param_grid_dl)
        }
    
    # Train goalkeepers models
        if df_gk.count() > 0:
            train_gk, test_gk = df_gk.randomSplit([0.8, 0.2], seed=42)
        
            for model_name, (pipeline, param_grid) in models_info.items():
                logger.info(f"Training {model_name} for goalkeepers...")
                cv = CrossValidator(
                    estimator=pipeline,
                    estimatorParamMaps=param_grid,
                    evaluator=evaluator,
                    numFolds=3
                )
                model = cv.fit(train_gk)
            
            # Store models
                if model_name == 'RandomForest':
                    self.model_rf_gk = model
                elif model_name == 'SVM':
                    self.model_svm_gk = model
                elif model_name == 'XGBoost':
                    self.model_xgb_gk = model
                elif model_name == 'DeepLearning':
                    self.model_dl_gk = model
            
            # Evaluate
                predictions = model.transform(test_gk)
                predictions = predictions.withColumn("Value_euros", expm1(col("Value_log")))
                predictions = predictions.withColumn("Prediction_euros", expm1(col("prediction")))
                rmse = evaluator.evaluate(predictions)
            
                logger.info(f"RMSE {model_name} gardiens: {rmse}")
            
            # Save predictions
                predictions.select("Name", "Value_euros", "Prediction_euros").coalesce(1).write.csv(
                    f"hdfs://localhost:9000/football/data/predictions_gk_{model_name.lower()}.csv", 
                    header=True, mode="overwrite")
        else:
            logger.warning("Aucune donnée pour les gardiens")
    
    # Train field players models
        train_field, test_field = df_field.randomSplit([0.8, 0.2], seed=42)
    
        for model_name, (pipeline, param_grid) in models_info.items():
            logger.info(f"Training {model_name} for field players...")
            cv = CrossValidator(
                estimator=pipeline,
                estimatorParamMaps=param_grid,
                evaluator=evaluator,
                numFolds=2
            )
            model = cv.fit(train_field)
        
        # Store models
            if model_name == 'RandomForest':
                self.model_rf_field = model
            elif model_name == 'SVM':
                self.model_svm_field = model
            elif model_name == 'XGBoost':
                self.model_xgb_field = model
            elif model_name == 'DeepLearning':
                self.model_dl_field = model
        
        # Evaluate
            predictions = model.transform(test_field)
            predictions = predictions.withColumn("Value_euros", expm1(col("Value_log")))
            predictions = predictions.withColumn("Prediction_euros", expm1(col("prediction")))
            rmse = evaluator.evaluate(predictions)
        
            logger.info(f"RMSE {model_name} joueurs de champ: {rmse}")
        
        # Save predictions
            predictions.select("Name", "Value_euros", "Prediction_euros").coalesce(1).write.csv(
                f"hdfs://localhost:9000/football/data/predictions_field_{model_name.lower()}.csv", 
                header=True, mode="overwrite")
    
    # Save scalers
        if df_gk.count() > 0:
            gk_scaler.write().overwrite().save("hdfs://localhost:9000/football/models/scaler_gk")
            logger.info("✅ Scaler model for goalkeepers saved to HDFS")
    
        field_scaler.write().overwrite().save("hdfs://localhost:9000/football/models/scaler_field")
        logger.info("✅ Scaler model for field players saved to HDFS")
    
        logger.info(f"Temps d'entraînement total: {time.time() - start_time:.2f} secondes")
    
    # Return instance attributes
        return (self.model_rf_gk, self.model_svm_gk, self.model_xgb_gk, self.model_dl_gk, gk_features, self.model_rf_field, self.model_svm_field, self.model_xgb_field, self.model_dl_field, field_features)        

    def save_model(self, model, path):
        """Save model to HDFS"""
        if model is not None:
            try:
                # For CrossValidatorModel, save the best model
                if hasattr(model, 'bestModel'):
                    best_model = model.bestModel.stages[-1]
                    best_model.write().overwrite().save(f"hdfs://localhost:9000/football/models/{path}")
                else:
                    model.write().overwrite().save(f"hdfs://localhost:9000/football/models/{path}")
                logger.info(f"Modèle sauvegardé dans hdfs://localhost:9000/football/models/{path}")
            except Exception as e:
                logger.error(f"Erreur lors de la sauvegarde du modèle {path}: {e}")
        else:
            logger.warning(f"Aucun modèle à sauvegarder pour {path}")

    def load_model(self, path, model_type='RandomForest'):
        """Load model from HDFS"""
        try:
            if model_type == 'RandomForest' or model_type == 'XGBoost':
                return RandomForestRegressionModel.load(f"hdfs://localhost:9000/football/models/{path}")
            elif model_type == 'SVM' or model_type == 'DeepLearning':
                return LinearRegressionModel.load(f"hdfs://localhost:9000/football/models/{path}")
        except Exception as e:
            logger.error(f"Erreur lors du chargement du modèle {path}: {e}")
            return None

    def predict_player_value(self, player_data, is_goalkeeper, model_type='RandomForest'):
        """Predict player value for a Spark DataFrame using specified model type"""
        # Select appropriate model
        if is_goalkeeper:
            if model_type == 'RandomForest':
                model = self.model_rf_gk
            elif model_type == 'SVM':
                model = self.model_svm_gk
            elif model_type == 'XGBoost':
                model = self.model_xgb_gk
            elif model_type == 'DeepLearning':
                model = self.model_dl_gk
            else:
                model = self.model_rf_gk
        else:
            if model_type == 'RandomForest':
                model = self.model_rf_field
            elif model_type == 'SVM':
                model = self.model_svm_field
            elif model_type == 'XGBoost':
                model = self.model_xgb_field
            elif model_type == 'DeepLearning':
                model = self.model_dl_field
            else:
                model = self.model_rf_field
        
        if model is None:
            logger.warning(f"Modèle {model_type} non disponible pour {'gardiens' if is_goalkeeper else 'joueurs de champ'}")
            return None
        
        # Preprocess the DataFrame
        player_df, feature_columns, scaler_model = self.preprocess_data(
            player_data, is_goalkeeper, use_combined_scaler=True
        )
        
        # Make predictions
        predictions = model.transform(player_df)
        predictions = predictions.withColumn("Prediction_euros", expm1(col("prediction")))
        
        # Apply correction factor for Kafka predictions (if applicable)
        correction_factor = 3.418825E7 / 3441558.4048566045
        predictions = predictions.withColumn("Prediction_euros", col("Prediction_euros") * correction_factor)
        logger.info(f"Applied correction factor {correction_factor} to {model_type} predictions")
        
        logger.info(f"Échantillon de prédictions {model_type} ({'gardiens' if is_goalkeeper else 'joueurs de champ'}):")
        predictions.select("Name", "Prediction_euros").show(5)
        
        return predictions

    def start_kafka_listener(self):
        """Start Kafka listener in a separate thread for real-time predictions"""
        try:
            consumer = KafkaConsumer('players-data', bootstrap_servers=['127.0.0.1:9092'])
            logger.info("Kafka listener started for real-time predictions")
            
            while True:
                for message in consumer:
                    try:
                        data = json.loads(message.value.decode('utf-8'))
                        logger.info(f"Processing Kafka message: {data['Name']}")
                        
                        # Create DataFrame from single message
                        pdf = pd.DataFrame([data])
                        df = self.spark.createDataFrame(pdf)
                        
                        is_goalkeeper = data.get('Position category', '').lower() == 'gardien'
                        
                        # Predict using all models
                        for model_type in ['RandomForest', 'SVM', 'XGBoost', 'DeepLearning']:
                            predictions = self.predict_player_value(df, is_goalkeeper, model_type)
                            if predictions is not None:
                                # Save predictions to HDFS with model type suffix
                                predictions.select("Name", "Prediction_euros").coalesce(1).write.csv(
                                    f"hdfs://localhost:9000/football/data/kafka_predictions_{model_type.lower()}.csv",
                                    header=True, mode="append"
                                )
                                logger.info(f"Real-time prediction for {data['Name']} with {model_type} saved to HDFS")
                    except Exception as e:
                        logger.error(f"Error processing Kafka message: {e}")
                
                time.sleep(5)  # Check every 5 seconds
        except Exception as e:
            logger.error(f"Kafka listener error: {e}")

   
    def analyze_feature_importance(self, model, feature_names, model_name):
        """Analyze feature importance"""
        if model is None:
            logger.warning(f"Aucun modèle pour analyser l'importance des features ({model_name})")
            return []
    
    # Check if the model is a RandomForest model
        rf_model = None
        if hasattr(model.bestModel.stages[-1], 'featureImportances'):
            rf_model = model.bestModel.stages[-1]
        elif hasattr(model.bestModel.stages[-1], 'coefficients'):  # For LinearRegression (SVM/DeepLearning)
            logger.info(f"Feature importance not available for {model_name}. Using coefficients instead.")
            importances = model.bestModel.stages[-1].coefficients.toArray()
            feature_importance = sorted(zip(feature_names, importances), key=lambda x: abs(x[1]), reverse=True)
        else:
            logger.warning(f"Feature importance not supported for {model_name}")
            return []

        if rf_model:
            importances = rf_model.featureImportances.toArray()
            feature_importance = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)
        elif hasattr(model.bestModel.stages[-1], 'coefficients'):
            feature_importance = feature_importance  # Already sorted above

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
            training_df = self.load_data_from_csv("/home/sami/Desktop/football_prediction_project/True_players_data.csv")
            self.create_combined_scaler(training_df, df)
            
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
            
            if self.model_rf_gk and df_gk.count() > 0:
                predictions_gk = self.predict_player_value(df_gk, is_goalkeeper=True, model_type='RandomForest')
                if predictions_gk:
                    predictions_gk.select("Name", "Prediction_euros").coalesce(1).write.csv(
                        "hdfs://localhost:9000/football/data/kafka_predictions_gk.csv", 
                        header=True, mode="overwrite"
                    )
                    logger.info("Prédictions Kafka gardiens sauvegardées dans kafka_predictions_gk.csv")
            
            if self.model_rf_field and df_field.count() > 0:
                predictions_field = self.predict_player_value(df_field, is_goalkeeper=False, model_type='RandomForest')
                if predictions_field:
                    predictions_field.select("Name", "Prediction_euros").coalesce(1).write.csv(
                        "hdfs://localhost:9000/football/data/kafka_predictions_field.csv", 
                        header=True, mode="overwrite"
                    )
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
        (model_rf_gk, model_svm_gk, model_xgb_gk, model_dl_gk, gk_features, model_rf_field, model_svm_field, model_xgb_field, model_dl_field, field_features) = predictor.train_model(df)


    except Exception as e:
        logger.error(f"Échec de l'entraînement: {e}")
        raise
    
    try:
        predictor.save_model(model_rf_gk, "player_value_model_rf_gk")
        predictor.save_model(model_svm_gk, "player_value_model_svm_gk")
        predictor.save_model(model_xgb_gk, "player_value_model_xgb_gk")
        predictor.save_model(model_dl_gk, "player_value_model_dl_gk")
        predictor.save_model(model_rf_field, "player_value_model_rf_field")
        predictor.save_model(model_svm_field, "player_value_model_svm_field")
        predictor.save_model(model_xgb_field, "player_value_model_xgb_field")
        predictor.save_model(model_dl_field, "player_value_model_dl_field")
    except Exception as e:
        logger.error(f"Échec de la sauvegarde des modèles: {e}")
        raise
    
    try:
        if model_rf_gk:
            predictor.analyze_feature_importance(model_rf_gk, gk_features, "goalkeeper_rf")
        if model_rf_field:
            predictor.analyze_feature_importance(model_rf_field, field_features, "field_rf")
        if model_svm_gk:
            predictor.analyze_feature_importance(model_svm_gk, gk_features, "goalkeeper_svm")
        if model_svm_field:
            predictor.analyze_feature_importance(model_svm_field, field_features, "field_svm")
        if model_xgb_gk:
            predictor.analyze_feature_importance(model_xgb_gk, gk_features, "goalkeeper_xgb")
        if model_xgb_field:
            predictor.analyze_feature_importance(model_xgb_field, field_features, "field_xgb")
        if model_dl_gk:
            predictor.analyze_feature_importance(model_dl_gk, gk_features, "goalkeeper_dl")
        if model_dl_field:
            predictor.analyze_feature_importance(model_dl_field, field_features, "field_dl")
    except Exception as e:
        logger.error(f"Échec de l'analyse des features: {e}")
        raise
    
    try:
        predictor.stream_predictions()
    except Exception as e:
        logger.error(f"Échec des prédictions en streaming: {e}")
        raise
    
    # Start Kafka listener
    threading.Thread(target=predictor.start_kafka_listener).start()
    
    # Keep the main thread alive (use input to prevent immediate exit)
    try:
        input("Press Enter to stop the application...\n")
    finally:
        predictor.spark.stop()
if __name__ == "__main__":
    main()

