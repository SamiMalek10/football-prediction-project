# Dans project_setup.sh, remplacer :
HADOOP_INSTALL_DIR="$HOME/opt"  # À changer en "$PROJECT_DIR"
PROJECT_DIR="$HOME/Desktop/football_prediction_project"
#!/bin/bash

# Define project directory
PROJECT_DIR="$HOME/Desktop/football_prediction_project"
HADOOP_INSTALL_DIR="$PROJECT_DIR"
KAFKA_INSTALL_DIR="$PROJECT_DIR"
SPARK_INSTALL_DIR="$PROJECT_DIR"

# Create directories
mkdir -p "$PROJECT_DIR/logs"
mkdir -p "$PROJECT_DIR/scripts"
mkdir -p "$PROJECT_DIR/notebooks"
"""
# Install dependencies
echo "Remove # if you want to installing dependencies..."
# sudo apt update
# sudo apt install -y openjdk-8-jdk python3-pip net-tools
# pip3 install pyspark kafka-python pandas plotly dash flask

# Set environment variables
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
echo "export HADOOP_HOME=$HADOOP_INSTALL_DIR/hadoop-3.3.6" >> ~/.bashrc
echo "export KAFKA_HOME=$KAFKA_INSTALL_DIR/kafka_2.3.1" >> ~/.bashrc
echo "export SPARK_HOME=$SPARK_INSTALL_DIR/spark-3.2.4-bin-hadoop3.2" >> ~/.bashrc
echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$KAFKA_HOME/bin:\$SPARK_HOME/bin" >> ~/.bashrc
source ~/.bashrc

echo "Project setup completed!"
