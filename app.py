import streamlit as st
from hdfs import InsecureClient
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler, StandardScalerModel

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FootballTransferFeeAppStreamlit") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# HDFS client
hdfs_client = InsecureClient('http://localhost:50070', user='sami')

# Load models and scalers from HDFS
gk_model_path = "hdfs://localhost:9000/football/models/player_value_model_rf_gk"
field_model_path = "hdfs://localhost:9000/football/models/player_value_model_rf_field"
gk_model = RandomForestRegressionModel.load(gk_model_path)
field_model = RandomForestRegressionModel.load(field_model_path)
scaler_gk = StandardScalerModel.load("hdfs://localhost:9000/football/models/scaler_gk")
scaler_field = StandardScalerModel.load("hdfs://localhost:9000/football/models/scaler_field")

# Load latest predictions from HDFS (non utilisé pour tendances/rapports)
def load_hdfs_data(path):
    files = hdfs_client.list(f'/football/data/{path}')
    part_file = next(f for f in files if f.startswith('part-00000'))
    with hdfs_client.read(f'/football/data/{path}/{part_file}', encoding='utf-8') as reader:
        df = pd.read_csv(reader)
        return df

# Charger les données (non utilisées pour les sections supprimées)
df_gk = load_hdfs_data('predictions_gk_randomforest.csv')
df_field = load_hdfs_data('predictions_field_randomforest.csv')

# Prediction function with improved validation
def predict_value(features, is_goalkeeper):
    # Validation des entrées avec conversion explicite
    required_fields = ['Age', 'Overall_rating', 'Potential', 'Wage']
    total_fields = ['Total_goalkeeping', 'Pace / Diving', 'Shooting / Handling', 
                   'Passing / Kicking', 'Dribbling / Reflexes'] if is_goalkeeper else \
                   ['Total_attacking', 'Total_skill', 'Total_movement', 
                    'Total_power', 'Total_mentality', 'Total_defending']
    assembler_inputs = required_fields + total_fields
    
    normalized_features = {}
    for field in required_fields:
        value = features.get(field, 0)
        st.write(f"Débogage - {field} reçu : {value}, type : {type(value)}")  # Débogage détaillé
        value = int(value)  # Conversion explicite en entier
        if field == 'Wage':
            if not 0 <= value <= 200000:
                raise ValueError(f"{field} must be between 0 and 200,000")
        else:  # Age, Overall_rating, Potential
            expected_range = (15, 40) if field == 'Age' else (50, 99)
            if not expected_range[0] <= value <= expected_range[1]:
                raise ValueError(f"{field} must be between {expected_range[0]} and {expected_range[1]}")
        normalized_features[field] = value
    for field in total_fields:
        value = features.get(field, 1)
        st.write(f"Débogage - {field} reçu : {value}, type : {type(value)}")  # Débogage détaillé
        value = int(value)  # Conversion explicite en entier
        if not 1 <= value <= 99:
            raise ValueError(f"{field} must be between 1 and 99")
        normalized_features[field] = value

    data = spark.createDataFrame([normalized_features])
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol='features')
    assembled_data = assembler.transform(data).select('features')
    scaler = scaler_gk if is_goalkeeper else scaler_field
    scaled_data = scaler.transform(assembled_data).select('scaled_features')
    model = gk_model if is_goalkeeper else field_model
    prediction = model.transform(scaled_data).select('prediction').collect()[0][0]
    predicted_value = np.exp(prediction) * 1000 * 0.005  # Facteur ajusté
    st.write(f"Prediction brute: {prediction}, Valeur prédite: {predicted_value:,.2f} €")
    return predicted_value

# Streamlit UI
st.title("Prédiction de la Valeur d'un Joueur")

# Input fields with sliders
st.header("Informations du Joueur")
name = st.text_input("Nom du joueur", value="N. Woltemade")
age = st.slider("Âge", 15, 40, 22)
overall_rating = st.slider("Note générale", 50, 99, 76)
potential = st.slider("Potentiel", 50, 99, 82)
wage = st.slider("Salaire (€)", 0, 200000, 26000, step=1000)

position = st.selectbox("Position", ["Joueur de champ", "Gardien"])

if position == "Joueur de champ":
    total_attacking = st.slider("Total Attacking", 1, 99, 70)
    total_skill = st.slider("Total Skill", 1, 99, 65)
    total_movement = st.slider("Total Movement", 1, 99, 62)
    total_power = st.slider("Total Power", 1, 99, 74)
    total_mentality = st.slider("Total Mentality", 1, 99, 63)
    total_defending = st.slider("Total Defending", 1, 99, 21)
    features = {
        'Age': age, 'Overall_rating': overall_rating, 'Potential': potential, 'Wage': wage,
        'Total_attacking': total_attacking, 'Total_skill': total_skill, 'Total_movement': total_movement,
        'Total_power': total_power, 'Total_mentality': total_mentality, 'Total_defending': total_defending
    }
    is_goalkeeper = False
else:
    total_goalkeeping = st.slider("Total Goalkeeping", 1, 99, 85)
    pace_diving = st.slider("Pace / Diving", 1, 99, 75)
    shooting_handling = st.slider("Shooting / Handling", 1, 99, 70)
    passing_kicking = st.slider("Passing / Kicking", 1, 99, 75)
    dribbling_reflexes = st.slider("Dribbling / Reflexes", 1, 99, 76)
    features = {
        'Age': age, 'Overall_rating': overall_rating, 'Potential': potential, 'Wage': wage,
        'Total_goalkeeping': total_goalkeeping, 'Pace / Diving': pace_diving,
        'Shooting / Handling': shooting_handling, 'Passing / Kicking': passing_kicking,
        'Dribbling / Reflexes': dribbling_reflexes
    }
    is_goalkeeper = True

if st.button("Prédire"):
    try:
        predicted_value = predict_value(features, is_goalkeeper)
        st.success(f"La valeur prédite pour {name} est : {predicted_value:,.2f} €")
    except ValueError as e:
        st.error(str(e))

# Cleanup
spark.stop()
