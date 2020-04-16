


CREATE OR REPLACE TABLE dummy_v${parentIndex} (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      name       STRING,
      segments      ARRAY<STRING>
);


CREATE OR REPLACE TABLE dummy_v${parentIndex}_tmpl (
        id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      name       STRING,
      segments      ARRAY<STRING>
);


CREATE OR REPLACE TABLE dummy_v${parentIndex}_temp_tmpl (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      name       STRING,
      encoded_segments    STRING
);
