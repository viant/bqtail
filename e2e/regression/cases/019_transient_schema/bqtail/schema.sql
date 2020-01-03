

CREATE OR REPLACE TABLE dummy_v${parentIndex} (
      id            INT64 NOT NULL,
      type_id       INT64 NOT NULL,
      name          STRING,
      description   STRING
);



CREATE OR REPLACE TABLE dummy_v${parentIndex}_temp (
      id         INT64 NOT NULL,
      name       STRING,
      type_id    INT64 NOT NULL
);
