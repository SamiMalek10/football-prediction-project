#!/bin/bash
echo "🚀 Démarrage de l'écosystème Big Data..."

# Vérification des variables
source ~/.bashrc
cd ~/Desktop/football_prediction_project || { echo "❌ Répertoire incorrect"; exit 1; }

# Démarrer HDFS
echo "Démarrage HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh

# Démarrer YARN
echo "Démarrage YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

# Démarrer Zookeeper avec vérification
echo "Démarrage Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 10  # Augmenter le temps d'attente

# Vérifier que Zookeeper est actif
if ! nc -z localhost 2181; then
    echo "❌ Échec du démarrage de Zookeeper"
    exit 1
fi

# Démarrer Kafka avec vérification
echo "Démarrage Kafka..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
sleep 15  # Augmenter le temps d'attente

# Vérifier que Kafka est actif
if ! nc -z localhost 9092; then
    echo "❌ Échec du démarrage de Kafka"
    exit 1
fi


# Remplacer la partie création de topic par :
if ! $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 | grep -q "players-data"; then
    echo "Création du topic Kafka..."
    $KAFKA_HOME/bin/kafka-topics.sh --create \
        --topic players-data \
        --bootstrap-server 127.0.0.1:9092 \
        --partitions 3 \
        --replication-factor 1
else
    echo "Topic Kafka 'players-data' existe déjà"
fi


# Démarrer Spark
echo "Démarrage Spark..."
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077

echo "✅ Services démarrés avec succès"
echo "🌐 Interfaces:"
echo "HDFS: http://localhost:9870"
echo "Spark: http://localhost:8080"
