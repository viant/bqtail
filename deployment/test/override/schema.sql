
CREATE OR REPLACE TABLE dummy_v1 (
      ts         TIMESTAMP NOT NULL,
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      name       STRING
) PARTITION BY DATE(ts);
