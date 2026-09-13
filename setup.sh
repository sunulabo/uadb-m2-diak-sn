#!/bin/bash

echo "Démarrage infrastructure DIACK-SN..."

# Création dossiers
mkdir -p data dags models nifi_templates checkpoints

# -----------------------------
# ZOOKEEPER
# -----------------------------
echo " Lancement Zookeeper..."
sudo docker-compose up -d zookeeper

sleep 10

# -----------------------------
# KAFKA + NIFI + HBASE + HIVE
# -----------------------------
echo " Lancement Kafka / NiFi / HBase / Hive..."
sudo docker-compose up -d kafka nifi hbase hive-metastore postgres hive-server

sleep 60

# -----------------------------
# SPARK + AIRFLOW
# -----------------------------
echo " Lancement Spark + Airflow..."
sudo docker-compose up -d spark-master spark-worker

sleep 20

# -----------------------------
# CREATION TOPIC KAFKA
# -----------------------------
echo " Création topic Kafka..."

#sudo docker exec -it kafka kafka-topics \
#--create \
#--if-not-exists \
#--topic diak_cas_raw \
#--bootstrap-server kafka:9092 \
#--partitions 1 \
#--replication-factor 1

# -----------------------------
# HBASE
# -----------------------------
#echo " Initialisation HBase..."

#python hbase_setup.py || true

# -----------------------------
# PRODUCTEUR
# -----------------------------
echo " Lancement simulateur Kafka..."

#python kafka_producer_diak_sn.py 

echo ""
echo " Tout est prêt !"
echo ""
echo " Interfaces Web :"
echo "NiFi    : http://localhost:8081"
echo "Spark   : http://localhost:8080"
echo "HBase   : http://localhost:16010"
echo "Airflow : http://localhost:8082"