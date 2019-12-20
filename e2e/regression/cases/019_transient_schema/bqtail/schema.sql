

CREATE OR REPLACE TABLE dummy (
      id            INT64 NOT NULL,
      type_id       INT64 NOT NULL,
      name          STRING,
      description   STRING
);



CREATE OR REPLACE TABLE dummy_temp (
      id         INT64 NOT NULL,
      name       STRING,
      type_id    INT64 NOT NULL
);
