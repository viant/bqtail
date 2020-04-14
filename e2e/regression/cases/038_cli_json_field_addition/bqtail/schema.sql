CREATE OR REPLACE TABLE dummy_v${parentIndex} (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      params ARRAY<STRUCT<
        key string,
        text_value STRING>>
 );



CREATE OR REPLACE TABLE dummy_v${parentIndex}_tmpl (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      params ARRAY<STRUCT<
        key string,
        text_value STRING >>
 );

