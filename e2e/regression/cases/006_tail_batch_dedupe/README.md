### Ingestion with transient dataset 

### Scenario:

BqTail function is notified once data is upload to gs://${config.Bucket}/data/case005/dummy[1-2].json
It matches the the following route, to ingest data with transient table in temp dataset, followed by deduplicated final destination ingestion.


```json
 {
      "When": {
        "Prefix": "/data/case006",
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
      },
      "Async": true
}
```

### BqTail

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**:
    - [gs://${config.Bucket}/data/case005/dummy.json](data/dummy.json)

#### Output

* **Data:**
Big Query destination table:

```sql
SELECT * FROM bqtail.dummy
```
 
* **Logs:** 


[gs://${config.Bucket}/journal/dummy/${date}/$EventID.bqt](data/expect/journal.json)

### BqDispatch

