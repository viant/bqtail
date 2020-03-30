### Client side batch 

### Scenario:

This scenario tests dynamic schema field addition from JSON source files.

Initial schema is defined as

[schema.sql](bqtail/schema.sql)
```sql
CREATE OR REPLACE TABLE dummy_v${parentIndex}_tmpl (
      id         INT64 NOT NULL,
      type_id    INT64 NOT NULL
 );
```

The ingestion rule uses *AllowFieldAddition*: 

[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case038
  Suffix: .json

Dest:
  Table: bqtail.dummy_v038
  AllowFieldAddition: true
  Transient:
    Template: bqtail.dummy_v038_tmpl
    Dataset: temp
Batch:
  MultiPath: true
  Window:
    DurationInSec: 15

Async: true
OnSuccess:
  - Action: delete

```

The data file has the following JSON with extra fields (name, billable):
This files will be automatically add to transient table and Dest.Table
[dummy2.json](data/trigger/path2/dummy2.json)
```json
{"id": 101, "name": "dummy 1", "type_id": 1, "billable":  true}
{"id": 102, "name": "dummy 2", "type_id": 1}
{"id": 103, "name": "dummy 3", "type_id": 1}
{"id": 104, "name": "dummy 4", "type_id": 1}
{"id": 105, "name": "dummy 5", "type_id": 2}
{"id": 106, "name": "dummy 6", "type_id": 2}
{"id": 107, "name": "dummy 7", "type_id": 2}
{"id": 108, "name": "dummy 8", "type_id": 2}
{"id": 109, "name": "dummy 9", "type_id": 2, "billable":  true}
{"id": 110, "name": "dummy 10", "type_id": 1}
{"id": 111, "name": "dummy 11", "type_id": 1}
{"id": 112, "name": "dummy 12", "type_id": 1}
{"id": 113, "name": "dummy 13", "type_id": 3}
{"id": 114, "name": "dummy 14", "type_id": 3}
{"id": 115, "name": "dummy 15", "type_id": 3}
{"id": 116, "name": "dummy 16", "type_id": 3}
{"id": 117, "name": "dummy 17", "type_id": 1}
{"id": 118, "name": "dummy 18", "type_id": 1}
```

This test runs BqTail with CLI command.

```
 export GOOGLE_APPLICATION_CREDENTIALS='${env.HOME}/.secret/${gcpCredentials}.json'
 bqtail -s='${parent.path}/data/' -r=rule/rule.yaml 
```

