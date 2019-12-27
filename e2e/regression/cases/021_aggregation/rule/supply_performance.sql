SELECT
    DATE(timestamp) AS date,
    sku_id,
    supply_entity_id,
    MAX($EventID) AS batch_id,
    SUM( payment) payment,
    SUM((CASE WHEN type_id = 1 THEN t.payment + f.value WHEN type_id = 2 THEN t.payment * (1 + f.value) END)) charge,
    SUM(COALESCE(qty, 1.0)) AS qty
FROM $TempTable t
LEFT JOIN bqtail.fees f ON f.id = t.fee_id
GROUP BY 1, 2, 3
