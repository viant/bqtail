### Ingestion with transient dataset deduplication in async mode  

### Scenario:

This scenario tests data deduplication with basic table schema.


BqTail function is notified once data is uploaded to gs://${config.Bucket}/data/case006/dummy[1-2].json
It matches the the following rule, to ingest data with transient table in temp dataset,  then it deduplicate data in the destination table.


[@rule.json](rule.json)
```json
 [{
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
          "DurationInSec": 15
        }
      },
      "Async": true
   }]
```


Since table does not use nested column the following SQL is used for de duplications
```sql
SELECT id, MAX(type_id) AS type_id, MAX(name) AS name 
FROM temp.dummy_${eventId} 
GROUP BY 1
```


### BqTail

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**:
    - [gs://${config.Bucket}/data/case006/dummy1.json](data/trigger/dummy1.json)
    - [gs://${config.Bucket}/data/case006/dummy2.json](data/trigger/dummy2.json)

#### Output

* **Data:**
Big Query temp table:

```sql
SELECT * FROM temp.dummy
```

* **Batch window transient files**

- gs://${config.Bucket}/batch/dummy/

    - [${eventID1}.tnf](data/expect/batch/eventID1.tnf) 
    - [${eventID2}.tnf](data/expect/batch/eventID2.tnf)
    - [$WindowEndTimestamp.win](data/expect/batch/ts.win)

* **Defer task**:
  - [gs://${config.Bucket}/tasks/${jobID}.json](data/expect/tasks/dispatch.json)

Where jobID is created using the following:

$jobID: ${table}/${storageEventID}/dispatch

 
* **Logs:** 
  - gs://${config.Bucket}/journal/dummy/${date}/${jobID}/tail-job.json



### BqDispatch

BqDispatch function loads matched BigQuery JobID defer actions file from: [gs://${config.Bucket}/tasks/${jobID}.json](data/expect/tasks/dispatch.json)
to execute defer actions.

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}

* **Configuration:** 
    - [gs://e2e-data/config/bqtail.json](../../../config/bqdispatch.json)

* **Defer task**:
   - [gs://${config.Bucket}/tasks/${jobID}.json](data/expect/tasks/dispatch.json)


#### Output:

* **Logs:**
  - gs://${config.Bucket}/journal/dummy/${date}/${jobID}/dispatch.json
  - gs://${config.Bucket}/journal/dummy/${date}/${jobID}/dispatch-job.json
