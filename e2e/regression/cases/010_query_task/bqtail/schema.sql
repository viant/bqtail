

CREATE OR REPLACE TABLE dummy_v${parentIndex} (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      event_id   INT64,
      name       STRING
);


CREATE OR REPLACE TABLE summary_v${parentIndex} (
    event_id STRING,
    uris ARRAY<STRING>,
    row_count INT64,
    completed TIMESTAMP
);
