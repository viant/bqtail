### Synchronous data files ingestion

### Scenario:

BqTail function is notified once data is upload to gs://${config.Bucket}/data/case001/dummy.json
It matches the the following route to submit load Job to BiqQuery and wait till job is done synchronusly.

```json
 {
      "When": {
        "Prefix": "/data/case1",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "db1.dummy"
      }
    }
```



**Note:**

When BigQuery load time takes more than max cloud function execution time, the function is terminated, but BigQuery job continues.

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**: [gs://${config.Bucket}/data/case001/dummy.json](data/dummy.json)


#### Output

* **Data:**
Big Query destination table:

```sql
SELECT * FROM bqtail.dummy
```
 
* **Logs:** 
