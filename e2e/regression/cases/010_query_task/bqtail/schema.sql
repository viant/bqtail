

CREATE OR REPLACE TABLE dummy (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      event_id   INT64,
      name       STRING
);


CREATE OR REPLACE TABLE summary (
    event_id STRING,
    uris ARRAY<STRING>,
    row_count INT64,
    completed TIMESTAMP
);
