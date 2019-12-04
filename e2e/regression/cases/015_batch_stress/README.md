### Synchronous in batch data ingestion

### Scenario:

This scenario tests generate 2000k files to ingest for batch allocation stress testing.


[@rule.json](rule.json)
```json
[
  {
    "When": {
      "Prefix": "/data/case015",
      "Suffix": ".json"
    },
    "Async": true,
    "Dest": {
      "Table": "bqtail.dummy_$Mod(2)"
    },
    "Batch": {
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
