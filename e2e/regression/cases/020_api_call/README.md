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
{
  "When": {
    "Prefix": "/data/case020",
    "Suffix": ".json"
  },
  "Async": true,
  "Dest": {
    "Table": "bqtail.dummy_v20",
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
        "URL": "$callURL",
        "Method": "POST",
        "BodyURL": "${parentURL}/body.txt",
        "Auth": true
      }
    },
    {
      "Action": "query",
      "Request": {
        "SQLURL": "${parentURL}/summary.sql",
        "Dest": "bqtail.summary_20"
      }
    },
    {
      "Action": "delete"
    }
  ]
}

```

The call subsequent task can access the following attributes from call response with $ expression 

* StatusCode 
* Headers  http.Header
* Data: data structure dervied from http response body (if applicable)
* Body: raw http response


_**Note** that $Data.BatchID expression from from HTTP call response_