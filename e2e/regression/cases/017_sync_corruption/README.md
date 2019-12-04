### Corrupted data file in batch in sync mode

### Scenario:

This scenario tests batch ingestion with corrupted files in sync mode, the corrupted file are moved to $CorruptedURL
All valid data in batch is reprocess. 

[@rule.json](rule.json)
```json
[
  {
    "When": {
      "Prefix": "/data/case017",
      "Suffix": ".json"
    },
    "Dest": {
      "Table": "bqtail.dummy"
    },
    "Batch": {
      "RollOver": true,
      "Window": {
        "DurationInSec": 15
      }
    },
    "OnSuccess": [
      {
        "Action": "delete"
      }
    ]
  }
]
```
