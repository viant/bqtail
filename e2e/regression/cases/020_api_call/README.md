### API Call

### Scenario:

This scenario tests data ingestion with summary query in async mode with API call.

Once data is batch and loaded to temp table, API is call and response body contains BatchID that is used
in summary task 

for API cloud function which requires authentication is used.

where:

- HTTP call [body](rule/body.txt)
- query [SQL](rule/summary.sql)


[@rule.json](rule.json)
```json
[
  {
    "When": {
      "Prefix": "/data/case020",
      "Suffix": ".json"
    },
    "Async": true,
    "Dest": {
      "Table": "bqtail.dummy",
      "TransientDataset": "temp",
      "UniqueColumns": [
        "id"
      ]
    },
    "Batch": {
      "RollOver": true,
      "Window": {
        "DurationInSec": 15
      }
    },
    "OnSuccess": [
      {
        "Action": "call",
        "Request": {
          "BodyURL": "${parentURL}/body.txt",
          "URL": "$callURL",
          "Method": "POST",
          "Auth": true
        }
      },
      {
        "Action": "query",
        "Request": {
          "SQLURL": "${parentURL}/summary.sql",
          "Dest": "bqtail.summary"
        }
      },
      {
        "Action": "delete"
      }
    ]
  }
]
```

