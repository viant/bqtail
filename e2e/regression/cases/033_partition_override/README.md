### Templates

### Scenario:

This scenario test data ingestion where only one partition is overridden, while other date partition data is unchanged.

[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case${parentIndex}"
  Suffix: ".json"
Async: true
Batch:
  Window:
    DurationInSec: 10
Dest:
  Table: bqtail.transactions_v${parentIndex}
  Override: true
  Partition: $Date
  Transient:
    Dataset: temp
OnSuccess:
 - Action: delete
```

