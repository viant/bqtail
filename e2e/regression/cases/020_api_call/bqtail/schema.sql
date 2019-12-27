

CREATE OR REPLACE TABLE dummy (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      name       STRING
);

CREATE OR REPLACE TABLE summary (
    batch_id INT64,
    row_count INT64,
    completed TIMESTAMP
);
