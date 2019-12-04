### Asynchronous data ingestion

### Scenario:

This scenario tests individual asynchronous data ingestion, with delete post actions.



BqTail function is notified once data is uploaded to gs://${config.Bucket}/data/case002/dummy[1..2].json
It matches the the following rule data ingestion rule. 


[@rule.json](rule.json)
```json
 [{
      "When": {
        "Prefix": "/data/case002",
        "Suffix": ".json"
      },
      "Async": true,
      "Dest": {
        "Table": "bqtail.dummy"
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ]
}]
```

BqTail function does not wait for job completion, but instead if generate post action file with handled by BqDispatch once Big Query job completes.

**Note:**

BigQuery load time does not affect post action execution, since it is delegated to BqDispatch once LoadJob is completed.

### BqTail

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**:
    - [gs://${config.Bucket}/data/case002/dummy1.json](data/trigger/dummy1.json)
    - [gs://${config.Bucket}/data/case002/dummy2.json](data/trigger/dummy2.json)

#### Output



* **Data:**
Big Query destination table:

```sql
SELECT * FROM bqtail.dummy
```


* **Defer task**:
  - [gs://${config.Bucket}/tasks/${jobID}.json](data/expect/tasks/dispatch.json)

Where jobID is created using the following:

$jobID: ${table}/${storageEventID}/dispatch


 
* **Logs:** 

- gs://${config.Bucket}/Journal/Done/bqtail.dummy/$Date/$eventID.run




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

* **Data:**
Big Query destination table:

```sql
SELECT * FROM bqtail.dummy
```


* **Logs:** 

- gs://${config.Bucket}/Journal/Done/bqtail.dummy/$Date/$eventID.run
