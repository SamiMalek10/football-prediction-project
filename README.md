# 🏈 Projet Big Data Football - Prédiction Valeur des Joueurs

## 🎯 Objectif
Développer un système Big Data complet pour prédire la valeur marchande des joueurs de football en utilisant des technologies modernes.

## 🏗️ Architecture

```
Web Scraping (Sofifa) 
    ↓
Kafka Producer 
    ↓
Kafka 2.3.1 (Stream)
    ↓
Spark 3.5.1 (ML Pipeline)
    ↓
HDFS 2.7.3 ← → Hive 2.3.5
    ↓
Jupyter Dashboard
```

## 🚀 Installation et Démarrage

### 1. Installation Automatique
```bash
chmod +x project_setup.sh
./project_setup.sh
```

### 2. Activation de l'environnement
```bash
source ~/.bashrc
cd ~/football_prediction_project
source venv/bin/activate
```

### 3. Démarrage des services
```bash
./scripts/start-all.sh
```

### 4. Vérification des services
```bash
./scripts/check-services.sh
```

## 📊 Utilisation

### Pipeline Complet
```bash
cd ~/football_prediction_project
source venv/bin/activate
python scripts/main_pipeline.py
```

### Scraping Individuel
```python
python scripts/web_scraping_sofifa.py
```

### Entraînement ML
```python
python scripts/spark_ml_pipeline.py
```

### Dashboard
```bash
python scripts/dashboard.py
# Accès: http://localhost:8050
```

### Jupyter
```bash
jupyter notebook notebooks/
# Accès: http://localhost:8888
```

## 🌐 Interfaces Web

- **Hadoop NameNode**: http://localhost:9870
- **YARN ResourceManager**: http://localhost:8088  
- **Spark Master**: http://localhost:8080
- **Dashboard**: http://localhost:8050
- **Jupyter**: http://localhost:8888

## 🛑 Arrêt Propre des Services
bash
./scripts/stop-all.sh

🔍 Dépannage
Problèmes Courants
Ports déjà utilisés :

bash

sudo lsof -i :<port>  # Identifier le processus
sudo kill -9 <PID>    # Terminer le processus

Espace disque HDFS :

bash
hdfs dfs -df -h  # Vérifier l'espace
hdfs dfs -rm -r /tmp/*  # Nettoyer si nécessaire

Erreurs Kafka :

bash
# Redémarrer Zookeeper puis Kafka
$KAFKA_HOME/bin/zookeeper-server-stop.sh
$KAFKA_HOME/bin/kafka-server-stop.sh
sleep 5
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties


📚 Documentation Technique
Structure des Données

python
{
  "Name": "L. Messi",
  "Age": 36,
  "Overall_rating": 91,
  "Potential": 91,
  "Value": 25000000.0,  # en euros
  "Wage": 1600000.0,    # en euros
  "Pace": 85,
  "Shooting": 92,
  ... # autres stats
}

Variables d'Environnement Clés

bash
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
HADOOP_HOME=~/football_prediction_project/hadoop
SPARK_HOME=~/football_prediction_project/spark
KAFKA_HOME=~/football_prediction_project/kafka

📅 Roadmap
Intégration Airflow pour l'orchestration

Ajout d'une API REST pour les prédictions

Intégration continue (CI/CD)

Monitoring Prometheus/Grafana

📝 Licence
Projet open-source sous licence MIT.


Ce README complet intègre :
1. Toutes les informations du script bash d'installation
2. Les bonnes pratiques de documentation technique
3. Des sections de dépannage utiles
4. La roadmap pour l'évolution du projet
5. La structure des données et variables clés

Il est organisé pour permettre à un nouvel utilisateur de :
- Installer facilement le projet
- Comprendre l'architecture
- Utiliser les différentes composantes
- Résoudre les problèmes courants
- Contribuer au projet

# Football Player Value Prediction

This project predicts football player market values using a machine learning pipeline built with Apache Spark, Kafka, and HDFS. It processes player data, trains RandomForest models for goalkeepers and field players, and streams predictions via Kafka.

## Directory Structure
- `scripts/`: Python scripts (`main_pipeline.py`, `player_value_predictor.py`).
- `logs/`: Log files (excluded from Git).
- `True_players_data.csv`: Input dataset (excluded from Git).
- `predictions_*.csv`: Output predictions.
- `feature_importance_*.txt`: Feature importance reports.

## Setup
1. Install dependencies: `pip install -r requirements.txt`.
2. Start Hadoop: `/home/sami/opt/hadoop-3.3.6/sbin/start-dfs.sh`.
3. Start Kafka: `/home/sami/opt/kafka_2.3.1/bin/zookeeper-server-start.sh` and `kafka-server-start.sh`.
4. Run pipeline: `python scripts/main_pipeline.py`.

## Results
- Goalkeeper RMSE: 0.5893
- Field Player RMSE: 0.3331
- Training time: ~102s
