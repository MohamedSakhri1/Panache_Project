-- ============================================================
-- Analyse Robuste avec Partitionnement
-- Auteur: Mohamed
-- ============================================================

-- 0. Nettoyage
DROP TABLE IF EXISTS transactions_fraude;

-- 1. Création de la table PARTITIONNÉE
-- Note : La colonne 'ville' disparait de la liste principale 
-- et passe dans PARTITIONED BY.
CREATE EXTERNAL TABLE transactions_fraude (
    id INT,
    montant DOUBLE,
    `type` STRING,
    `timestamp` BIGINT,
    is_fraud BOOLEAN
)
PARTITIONED BY (ville STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/hive/warehouse/transactions';

-- 2. Découverte automatique des partitions
-- Cette commande scanne les dossiers (ville=Paris, ville=Lyon...)
-- et ignore automatiquement le dossier _spark_metadata !
ALTER TABLE transactions_fraude RECOVER PARTITIONS;

-- 3. Analyse (Le SQL reste le même, Impala gère la partition tout seul)
SELECT '--- CLASSEMENT DES VILLES (Données Partitionnées) ---' as rapport;
SELECT ville, count(*) as nb_fraudes, avg(montant) as montant_moyen
FROM transactions_fraude
WHERE is_fraud = true
GROUP BY ville
ORDER BY nb_fraudes DESC;

SELECT '--- EXEMPLE DE DATA ---' as rapport;
SELECT * FROM transactions_fraude LIMIT 3;