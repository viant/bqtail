### Transient project

### Scenario:

This scenario tests data ingestion with explicit transient project ProjectID, that is difference from CF default project.
Data is copied from transient dataset to destination table.
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
Async: true
OnSuccess:
 - Action: delete
```
