#!/bin/bash

# Script de lancement de l'ingestion Kafka
# Auteur: Mohamed
# Date: $(date)

echo "=========================================="
echo "   DEMARRAGE PIPELINE: INGESTION KAFKA    "
echo "=========================================="

# 1. Vérification de Python
if ! command -v python &> /dev/null; then
    echo "❌ Erreur: Python n'est pas installé."
    exit 1
fi

# 2. Installation des dépendances
echo "📦 Installation/Vérification des dépendances..."
pip install -r requirements.txt
if [ $? -ne 0 ]; then
    echo "❌ Erreur lors de l'installation des dépendances."
    exit 1
fi

# 3. Attente de Kafka (Optionnel mais recommandé)
echo "⏳ Vérification de la disponibilité du script..."

# 4. Lancement du producteur
echo "▶️  Lancement du producteur Python..."
python producer.py