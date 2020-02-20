
CREATE OR REPLACE TABLE dummy_v${parentIndex}_tmpl (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      name       STRING,
      use_case   STRING,
      date       DATE
);


CREATE OR REPLACE TABLE dummy_v${parentIndex} (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      name       STRING,
      use_case   STRING,
      exta       STRING,
      date       DATE
);


CREATE OR REPLACE TABLE dummy_v${parentIndex}_20200102 (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      name       STRING,
      use_case   STRING,
      exta       STRING,
      date       DATE
);

