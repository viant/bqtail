

CREATE OR REPLACE TABLE dummy_v${parentIndex} (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      name       STRING
);

CREATE OR REPLACE TABLE summary_v${parentIndex} (
    job_id STRING,
    row_count INT64,
    completed TIMESTAMP
);
