### Invalid schema data file in batch

### Scenario:

This scenario tests batch ingestion with incompatible scheme files, the corrupted or invalid scheme file are moved 
to $CorruptedURL or $InvalidSchemaURL respectively.

All valid data in batch is reprocess. 


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case022"
  Suffix: ".json"
Async: true
Dest:
  Table: bqtail.dummy
Batch:
  RollOver: true
  Window:
    DurationInSec: 15
OnSuccess:
  - Action: delete

```
