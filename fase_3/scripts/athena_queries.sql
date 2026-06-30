-- ============================================
-- Athena — Consultas sobre el Datalake de Fraude
-- ============================================
-- Requisito: tablas creadas en Glue Catalog (database: fraude_datalake)
--            Ejecutar desde Athena Console o CLI
-- ============================================

-- 1. Top 10 usuarios con más transacciones en un día
SELECT user_id, transaction_date, tx_count, daily_total, is_high_risk
FROM fraude_datalake.gold_risk_profile
ORDER BY tx_count DESC
LIMIT 10;

-- 2. Usuarios de alto riesgo (>12 transacciones/día)
SELECT user_id, transaction_date, tx_count, daily_total
FROM fraude_datalake.gold_risk_profile
WHERE is_high_risk = true
ORDER BY tx_count DESC;

-- 3. Volumen diario de transacciones (últimos 7 días)
SELECT transaction_date,
       COUNT(*) AS total_transacciones,
       ROUND(SUM(amount), 2) AS monto_total
FROM fraude_datalake.silver_transactions
WHERE transaction_date >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY transaction_date
ORDER BY transaction_date DESC;

-- 4. Total de usuarios únicos y % de alto riesgo
SELECT COUNT(DISTINCT user_id) AS total_usuarios,
       COUNT(DISTINCT CASE WHEN is_high_risk THEN user_id END) AS usuarios_high_risk,
       ROUND(COUNT(DISTINCT CASE WHEN is_high_risk THEN user_id END) * 100.0 /
             COUNT(DISTINCT user_id), 2) AS pct_high_risk
FROM fraude_datalake.gold_risk_profile;

-- 5. Transacciones por moneda (Silver)
SELECT currency, COUNT(*) AS cantidad, ROUND(AVG(amount), 2) AS monto_promedio
FROM fraude_datalake.silver_transactions
GROUP BY currency
ORDER BY cantidad DESC;

-- 6. Verificación: total filas en cada capa
SELECT 'Silver' AS capa, COUNT(*) AS filas FROM fraude_datalake.silver_transactions
UNION ALL
SELECT 'Gold', COUNT(*) FROM fraude_datalake.gold_risk_profile;
