### Query task

### Scenario:

This scenario tests data ingestion from temp to destination table, to fail on post load query task


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case026/"
  Suffix: ".json"
Dest:
  Table: bqtail.dummy_v26
  Transient:
    Dataset: temp
  UniqueColumns:
    - id
  Transform:
    event_id: "$EventID"
Async: true
OnSuccess:
  - Action: query
    Request:
      Append: true
      SQL: SELECT blah,  FROM $TempTable
      Dest: bqtail.summary_v26
  - Action: delete
```
