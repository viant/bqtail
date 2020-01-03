### Corrupted data file in batch

### Scenario:

This scenario tests batch ingestion with corrupted files, the corrupted file are moved to $CorruptedURL
All valid data in batch is reloaded. 
If at lease there is one valid file to reload, the job status is ok, otherwise error.


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case021"
  Suffix: ".json"
Async: true
Dest:
  Table: bqtail.edummy_v21
Batch:
  RollOver: true
  Window:
    DurationInSec: 15
CorruptedFileURL: gs://${opsBucket}/BqTail/Corrupted/dummy_v21
InvalidSchemaURL: gs://${opsBucket}/BqTail/Schema/dummy_v21
OnSuccess:
  - Action: delete
```


