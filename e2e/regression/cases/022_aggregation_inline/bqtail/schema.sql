

CREATE OR REPLACE TABLE transactions (
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
);


CREATE OR REPLACE TABLE fees (
      id         INT64,
      type_id    INT64,
      value      FLOAT64
);


CREATE OR REPLACE TABLE supply_performance (
    date DATE,
    batch_id INT64,
    sku_id INT64,
    supply_entity_id INT64,
    payment FLOAT64,
    qty FLOAT64,
    charge FLOAT64
) PARTITION BY date CLUSTER BY supply_entity_id, sku_id;

