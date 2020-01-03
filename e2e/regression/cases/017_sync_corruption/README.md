### Corrupted data file in batch in sync mode

### Scenario:

This scenario tests batch ingestion with corrupted files in sync mode, the corrupted file are moved to $CorruptedURL
All valid data in batch is reprocess. 

[@rule.yaml](rule/rule.yaml)
```json
When:
  Prefix: /data/case${parentIndex}/
  Suffix: .json
Dest:
  Table: bqtail.dummy_v${parentIndex}
Batch:
  RollOver: true
  Window:
    DurationInSec: 15
OnSuccess:
  - Action: delete

```
