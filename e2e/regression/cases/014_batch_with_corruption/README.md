### Corrupted data file in batch

### Scenario:

This scenario tests batch ingestion with corrupted files, the corrupted file are moved to $CorruptedURL
All valid data in batch is reprocess. 

[@rule.json](rule.json)
```json
[
  {
    "When": {
      "Prefix": "/data/case014",
      "Suffix": ".json"
    },
    "Async": true,
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


