### Query task

### Scenario:

This scenario tests data ingestion with summary query in sync mode that produces batch row count, batching event id and 
source file URIs.


You can use the following variables in the SQL:

- $DestTable: destination table
- $TempTable: temp table
- $EventID: storage event id triggering load or batch
- $URLs: coma separated list of load URIs
- $SourceURI: one of load URI
- $RuleURL: transfer rule URL




BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case010/dummy[1..2].json
It matches the the following rule to submit load Job to BiqQuery. 

[@rule.json](rule.json)
```json
[{
    "When": {
      "Prefix": "/data/case010",
      "Suffix": ".json"
    },
    "Dest": {
      "Table": "bqtail.dummy",
      "Transient":{"Dataset": "temp"},
      "UniqueColumns": ["id"],
      "Transform": {
        "event_id": "$EventID"
      }
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
          "Append": true,
          "SQL": "SELECT '$EventID' AS event_id, SPLIT('$URLs', ',') AS uris, COUNT(1) AS row_count, CURRENT_TIMESTAMP() AS completed FROM $TempTable",
          "Dest": "bqtail.summary"
        }
      }
    ]
}]
```

