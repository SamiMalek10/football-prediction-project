#!/bin/bash
echo "🔍 Vérification des services Big Data..."

# Installation de net-tools si absent
if ! command -v netstat &> /dev/null; then
    echo "⚠ Installation de net-tools..."
    sudo apt-get install -y net-tools
fi

echo "=== Services Java ==="
jps || { echo "❌ Java non installé"; exit 1; }

echo -e "\n=== Ports en écoute ==="
{
    echo "HDFS NameNode (9870):"
    netstat -tlnp | grep :9870 || echo "❌ Non actif"
    
    echo "YARN ResourceManager (8088):"
    netstat -tlnp | grep :8088 || echo "❌ Non actif"
    
    echo "Spark Master (8080):"
    netstat -tlnp | grep :8080 || echo "❌ Non actif"
    
    echo "Kafka (9092):"
    netstat -tlnp | grep :9092 || echo "❌ Non actif"
} 2>/dev/null || echo "⚠ Impossible de vérifier les ports"

# ... reste du script inchangé ...

echo -e "\n=== Test HDFS ==="
$HADOOP_HOME/bin/hdfs dfs -ls / 2>/dev/null && echo "✅ HDFS OK" || echo "❌ HDFS KO"

echo -e "\n=== Test Kafka ==="
timeout 5 $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 2>/dev/null && echo "✅ Kafka OK" || echo "❌ Kafka KO"
