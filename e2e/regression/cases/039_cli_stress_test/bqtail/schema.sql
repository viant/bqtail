
CREATE OR REPLACE TABLE dummy_v${parentIndex}_tmpl (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      event_type   INT64 NOT NULL,
      ts          TIMESTAMP,
      name        STRING
)   PARTITION BY DATE(ts) CLUSTER BY type_id;



CREATE OR REPLACE TABLE dummy_v${parentIndex}_v1 (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      event_type   INT64 NOT NULL,
      ts          TIMESTAMP,
      name        STRING
)   PARTITION BY DATE(ts) CLUSTER BY type_id;

