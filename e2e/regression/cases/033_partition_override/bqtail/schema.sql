CREATE OR REPLACE TABLE transactions_v${parentIndex} (
      id         STRING,
      timestamp   TIMESTAMP,
      batch_id    INT64,
      event_id    INT64,
      sku_id INT64,
      supply_entity_id INT64,
      payment FLOAT64,
      demany_entity_id INT64,
      charge FLOAT64,
      qty FLOAT64,
      fee_id INT64
) PARTITION BY DATE(timestamp) CLUSTER BY event_id;