-- hive_setup.sql — Tables Hive Diak-SN 
CREATE DATABASE IF NOT EXISTS diak_sn COMMENT 'Santé Sénégal — UADB 2025'; 
USE diak_sn; 
  
CREATE TABLE IF NOT EXISTS cas_journaliers ( 
    cas_id_secure STRING, district STRING, pathologie STRING, 
    age_tranche STRING, sexe STRING, gravite STRING, ingestion_ts TIMESTAMP 
) PARTITIONED BY (date_obs STRING, pathologie_part STRING) 
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY'); 
  
CREATE TABLE IF NOT EXISTS stocks_medicaments ( 
    district STRING, medicament STRING, 
    stock_actuel INT, stock_mini INT, ingestion_ts TIMESTAMP 
) STORED AS ORC; 
  -- Vue : Alertes épidémie par district 
CREATE OR REPLACE VIEW vue_alertes_districts AS 
SELECT district, pathologie, 
    COUNT(*) AS nb_cas_7j, 
    CASE WHEN COUNT(*) > 100 THEN 'CRITIQUE' 
         WHEN COUNT(*) > 50  THEN 'ALERTE' 
         ELSE 'NORMAL' END AS statut, 
    MIN(ingestion_ts) AS premier_cas 
FROM cas_journaliers 
WHERE date_obs >= DATE_SUB(CURRENT_DATE(),7) 
GROUP BY district, pathologie 
ORDER BY nb_cas_7j DESC; 
  -- Vue : Ruptures de stock imminentes 
CREATE OR REPLACE VIEW vue_ruptures_stock AS 
SELECT district, medicament, stock_actuel, stock_mini, 
    ROUND((stock_actuel / COALESCE(stock_mini,1)) * 100, 1) AS pct_stock 
FROM stocks_medicaments 
WHERE stock_actuel <= stock_mini * 1.2  -- Seuil : 20% au-dessus du minimum 
ORDER BY pct_stock ASC; 