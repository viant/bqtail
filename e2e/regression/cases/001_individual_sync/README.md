### Synchronous data files ingestion

### Scenario:

This scenario tests individual synchronous data ingestion, without post actions.

BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case001/dummy.json
It matches the the following rule data ingestion rule.


[@rule.json](rule.json)
```json
 [{
      "When": {
        "Prefix": "/data/case1",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "db1.dummy"
      }
 }]
```



**Note:**

In case BigQuery load time takes more than max cloud function execution time, the function is terminated, but BigQuery job continues.


#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**: [gs://${triggerBucket}/data/case001/dummy.json](data/trigger/dummy.json)


#### Output

* **Data:**
Big Query destination table:

```sql
SELECT * FROM bqtail.dummy
```
 
* **Logs:** 
  - ${JournalURL}/Running/bqtail.dummy/$Date/${eventID}.run
    
* **Stack driver**
  - Response status