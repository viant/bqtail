### Synchronous in batch data ingestion

### Scenario:

This scenario tests generate 2000k files to ingest for batch allocation stress testing.


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case${parentIndex}"
  Suffix: ".json"
Async: true
Dest:
  Table: bqtail.dummy_v${parentIndex}_$Mod(2)
Batch:
  Window:
    DurationInSec: 15
OnSuccess:
  - Action: delete

```
