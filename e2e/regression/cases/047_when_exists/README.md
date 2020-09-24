### Asynchronous in batch data ingestion

### Scenario:

This scenario tests post action with When and table Exists criteria to run action.


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case${parentIndex}/
  Suffix: .json
Dest:
  Table: bqtail.dummy_v${parentIndex}
  Transient:
    Dataset: temp

Batch:
  RollOver: true
  MultiPath: true
  Window:
    DurationInSec: 15
Async: true
OnSuccess:
  - Action: query
    When:
      Exits: bqtail.dummy_dep1_v${parentIndex}
    Request:
      SQL: SELECT * FROM $TempTable
      Dest: bqtail.dummy_dep1_v${parentIndex}
  - Action: query
    When:
      Exits: bqtail.dummy_dep2_v${parentIndex}
    Request:
      SQL: SELECT * FROM $TempTable
      Dest: bqtail.dummy_dep2_v${parentIndex}
  - Action: delete
```
