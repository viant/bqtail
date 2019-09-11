### Data export trigger by Query with destination 

### Scenario:

BqTail function is notified once data is upload to gs://${config.Bucket}/data/case007/dummy[1-2].json
It matches the the following route, to ingest data with transient table in temp dataset, followed by deduplicated final destination ingestion.
In this scenario deduplication process has to handle nested [data structure](data/dummy1.json)
 

```json
 {
      "When": {
        "Prefix": "/data/case007",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "bqtail.dummy",
        "TransientDataset": "temp",
        "UniqueColumns": ["id"]
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ],
      "Batch": {
        "Window": {
          "DurationInSec": 10
        }
      }
}
```

