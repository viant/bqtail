

CREATE OR REPLACE TABLE dummy (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL,
      event_type STRING,
      name       STRING
);


CREATE OR REPLACE TABLE event_types (
      id         INT64 NOT NULL,
      name       STRING
);

