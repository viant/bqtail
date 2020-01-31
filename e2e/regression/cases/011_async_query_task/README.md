### Async query task 

### Scenario:

This scenario tests data ingestion with summary query in async mode.


BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case011/dummy[1..2].json
It matches the the following rule to submit load Job to BiqQuery. 


[@rule.json](rule.json)
```json
[
  {
    "When": {
      "Prefix": "/data/case011",
      "Suffix": ".json"
    },
    "Async": true,
    "Dest": {
      "Table": "bqtail.dummy",
      "Transient":{"Dataset": "temp"},
      "UniqueColumns": ["id"]
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
      },
      {
        "Action": "query",
        "Request": {
          "SQL": "SELECT '$JobID' AS job_id, COUNT(1) AS row_count, CURRENT_TIMESTAMP() AS completed FROM $DestTable",
          "Dest": "bqtail.summary"
        }
      }
    ]
  }
]
```

