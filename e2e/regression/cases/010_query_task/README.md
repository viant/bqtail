### Query task

### Scenario:

This scenario tests data ingestion with summary query in sync mode.


BqTail function is notified once data is upload to gs://${config.Bucket}/data/case010/dummy[1..2].json
It matches the the following route to submit load Job to BiqQuery. 

```json
{
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
}
```

