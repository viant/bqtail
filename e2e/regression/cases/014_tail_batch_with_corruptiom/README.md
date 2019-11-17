### Synchronous in batch data ingestion

### Scenario:

This scenario tests data sync data ingestion in batch.


BqTail function is notified once data is upload to gs://${config.Bucket}/data/case003/dummy[1..2].json
It matches the the following rule, to ingest data in batch. All function write event sourceURL to batchURL base location.
Only the one function will  wait for the whole batch duration to run BigQuery Load Job with batch sourceURLs and post action after job is completed.
Other function within batch do not wait or submit load job. 



[@rule.json](rule.json)
```json
 [{
      "When": {
        "Prefix": "/data/case003",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "bqtail.dummy"
      },
      "Batch": {
        "Window": {
          "DurationInSec": 10
        }
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ]
   }]
```

**Note:**

When BigQuery load time takes more than max cloud function execution time, the function is terminated, but BigQuery job continues.

### BqTail

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**:
    - [gs://${config.Bucket}/data/case003/dummy1.json](data/trigger/dummy1.json)
    - [gs://${config.Bucket}/data/case003/dummy2.json](data/trigger/dummy2.json)

#### Output

 
* **Batch window transient files**

- gs://${config.Bucket}/batch/dummy/

    - [${eventID1}.tnf](data/expect/batch/eventID1.tnf) 
    - [${eventID2}.tnf](data/expect/batch/eventID2.tnf)
    - [$WindowEndTimestamp.win](data/expect/batch/ts.win)


* **Data:**
Big Query destination table:

```sql
SELECT * FROM bqtail.dummy
```


* **Logs:** 

    - gs://${config.Bucket}/journal/dummy/${date}/

### BqDispatch

N/A