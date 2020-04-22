

CREATE OR REPLACE TABLE dummy_v${parentIndex} (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      name       STRING
);



CREATE OR REPLACE TABLE dummy_v${parentIndex}_journal (
      EventID STRING,
      URLS ARRAY<STRING>,
      TableName STRING,
      Records INT64,
      Loaded TIMESTAMP
);

