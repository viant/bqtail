### Transient project

### Scenario:

This scenario tests data ingestion with explicit transient project ProjectID, that is difference from CF default project.
Make sure that you have setup separate e2e project with credentials (run.yaml -> bqCredentials), remove skip.txt


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case${parentIndex}/"
  Suffix: ".json"
Dest:
  Table: ${projectID}:bqtail.dummy_v${parentIndex}
  Transient:
    Dataset: temp
    ProjectID: '${bqProjectID}'
  UniqueColumns:
    - id
  Transform:
    event_id: "$EventID"
Async: true


OnSuccess:
  - Action: query
    Request:
      Append: true
      SQL: SELECT '$EventID' AS event_id, SPLIT('$URLs', ',') AS uris, COUNT(1) AS row_count, CURRENT_TIMESTAMP() AS completed FROM $TempTable
      Dest: ${projectID}:bqtail.summary_v${parentIndex}
  - Action: delete

```
