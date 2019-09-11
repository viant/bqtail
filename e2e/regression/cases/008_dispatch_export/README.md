### Ingestion with transient dataset 

### Scenario:

BqTail function is notified once data is upload to gs://${config.Bucket}/data/case007/dummy[1-2].json
It matches the the following route, to ingest data with transient table in temp dataset, followed by deduplicated final destination ingestion.
In this scenario deduplication process has to handle nested [data structure](data/dummy1.json)
 

```json
  {
    "When": {
      "Dest": ".+:bqdispatch\\.dummy_v2",
      "Type": "QUERY"
    },
    "OnSuccess": [
      {
        "Action": "export",
        "Request": {
          "DestURL": "gs://${config.Bucket}/export/dummy.json.gz"
        }
      }
    ]
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

N/A