### Transient project

### Scenario:

This scenario tests data ingestion with explicit transient projects that are load balanced.
Make sure that you have setup separate e2e project with credentials (run.yaml -> bqCredentials), remove skip.txt

[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case${parentIndex}"
  Suffix: ".json"
Async: true
Dest:
  Pattern: /data/case${parentIndex}/(\\d{4})/(\\d{2})/(\\d{2})/.+
  Table: bqtail.dummy_v${parentIndex}_$Mod(2)_$1$2$3
  Transient:
    Dataset: temp
    Balancer:
      Strategy: rand
      MaxLoadJobs: 2
      ProjectIDs:
        - ${projectID}
        - ${bqProjectID}
Batch:
  Window:
    MultiPath: true
    DurationInSec: 10
OnSuccess:
  - Action: delete
```
