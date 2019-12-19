### Query task

### Scenario:

This scenario tests data ingestion with summary query in sync mode.


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
        "TransientDataset": "temp",
        "UniqueColumns": ["id"]
      },
      "Batch": {
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
}]
```

