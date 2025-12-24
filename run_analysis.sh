#!/bin/bash

# Script d'analyse SQL via Impala
# Auteur: Mohamed
# Date: $(date)

echo "=========================================="
echo "   DEMARRAGE PIPELINE: ANALYSE IMPALA     "
echo "=========================================="

# Vérification que le conteneur tourne
if [ ! "$(docker ps -q -f name=impala)" ]; then
    echo "❌ Erreur : Le conteneur Impala ne tourne pas."
    exit 1
fi

echo "🚀 Exécution du script SQL fraud_analysis.sql sur Impala..."
echo "----------------------------------------------------------"

# On utilise l'option -i pour passer le fichier local vers le conteneur via stdin
# L'option -f - dit à Impala de lire le fichier depuis l'entrée standard
docker exec -i impala impala-shell -f - < fraud_analysis.sql

echo "----------------------------------------------------------"
echo "✅ Analyse terminée."