


CREATE OR REPLACE TABLE dummy_v${parentIndex} (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      created    TIMESTAMP,
      name       STRING
) PARTITION BY DATE(created);


CREATE OR REPLACE TABLE dummy_v${parentIndex}_transient (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      created    TIMESTAMP,
      name       STRING
);
