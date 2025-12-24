import time
import json
import random
from kafka import KafkaProducer

# Configuration du producteur
# On tape sur localhost:9092 car on est à l'extérieur des conteneurs
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPIC_NAME = 'transactions'
CITIES = ['Paris', 'Lyon', 'Marseille', 'Bordeaux', 'Lille']
TYPES = ['CB', 'Virement', 'Retrait', 'Prelevement']

print(f"--- Démarrage de l'envoi vers le topic '{TOPIC_NAME}' ---")

try:
    transaction_id = 1
    while True:
        # Simulation d'une transaction
        transaction = {
            "id": transaction_id,
            "montant": round(random.uniform(10.0, 15000.0), 2), # Montant entre 10 et 15000
            "ville": random.choice(CITIES),
            "type": random.choice(TYPES),
            "timestamp": int(time.time())
        }

        # Envoi
        producer.send(TOPIC_NAME, value=transaction)
        
        # Log pour voir ce qui se passe
        if transaction['montant'] > 10000:
            print(f"⚠️  Envoyé (SUSPECT): {transaction}")
        else:
            print(f"✅ Envoyé: {transaction}")

        transaction_id += 1
        time.sleep(1) # Une transaction par seconde

except KeyboardInterrupt:
    print("Arrêt du producteur.")
    producer.close()