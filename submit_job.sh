#!/bin/bash

# Script de compilation et soumission Spark
# Auteur: Mohamed
# Date: $(date)

PROJECT_DIR="./project-data"
JAR_PATH="//project-data/target/scala-2.12/frauddetection_2.12-1.0.jar"
RESET_MODE=false

# Vérification des arguments
for arg in "$@"
do
    if [ "$arg" == "refresh" ] || [ "$arg" == "--reset" ]; then
        RESET_MODE=true
    fi
done

echo "=========================================="
echo "   DEMARRAGE PIPELINE: TRAITEMENT SPARK   "
echo "=========================================="

if [ "$RESET_MODE" = true ]; then
    echo "🧹 Mode RESET activé : Nettoyage du checkpoint HDFS..."
    # Utilisation du chemin complet HDFS pour éviter les soucis de conversion de chemin Git Bash
    docker exec namenode hdfs dfs -rm -r -f hdfs://namenode:8020/tmp/checkpoints/transactions
    echo "✅ Checkpoint nettoyé."
fi

# 1. Préparation de la structure du projet (Requis par SBT)
echo "📂 Organisation des fichiers sources..."
if [ -f "$PROJECT_DIR/FraudDetector.scala" ]; then
    mkdir -p "$PROJECT_DIR/src/main/scala"
    mv "$PROJECT_DIR/FraudDetector.scala" "$PROJECT_DIR/src/main/scala/"
    echo "✅ Fichier Scala déplacé dans src/main/scala"
fi

LOCAL_JAR_PATH="$PROJECT_DIR/target/scala-2.12/frauddetection_2.12-1.0.jar"

if [ -f "$LOCAL_JAR_PATH" ]; then
    echo "⚡ Le JAR existe déjà ($LOCAL_JAR_PATH). On saute la compilation."
else
    echo "🔨 Compilation du code Scala (via conteneur SBT)..."
    echo "   Cela peut prendre une minute la première fois..."

    docker run --rm \
      -v "$(cygpath -w "$PWD/project-data"):/app" \
      -w //app \
      sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.8.2_2.12.17 \
      sbt package

    if [ $? -ne 0 ]; then
        echo "❌ Erreur de compilation."
        exit 1
    fi
    echo "✅ Compilation réussie !"
fi

# 3. Soumission du Job à Spark
echo "🚀 Soumission du job au Spark Master..."

docker exec -u 0 spark-master //opt/spark/bin/spark-submit \
  --verbose \
  --class FraudDetector \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  $JAR_PATH