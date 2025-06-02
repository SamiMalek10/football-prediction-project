#!/bin/bash
echo "🛑 Arrêt de l'écosystème Big Data..."

# Source des variables d'environnement
source ~/.bashrc
cd Desktop/football_prediction_project/

# Arrêt Spark
echo "Arrêt Spark..."
$SPARK_HOME/sbin/stop-worker.sh
$SPARK_HOME/sbin/stop-master.sh

# Arrêt Kafka
echo "Arrêt Kafka..."
$KAFKA_HOME/bin/kafka-server-stop.sh

# Arrêt Zookeeper
echo "Arrêt Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-stop.sh

# Arrêt YARN
echo "Arrêt YARN..."
$HADOOP_HOME/sbin/stop-yarn.sh

# Arrêt HDFS
echo "Arrêt HDFS..."
$HADOOP_HOME/sbin/stop-dfs.sh

echo "✅ Tous les services sont arrêtés!"
