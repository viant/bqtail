### Asynchronous in batch data ingestion

### Scenario:

BqTail function is notified once data is upload to gs://${config.Bucket}/data/case004/dummy[1..2].json
It matches the the following route, to ingest data in batch. All BqTail cloud functions write event sourceURL to ${batchURL}/$dest/ base location.
Only one function manages the batching while the other quite, once batch window is closed function runs BigQuery Load Job with batch sourceURLs and defer actions.


```json
  {
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
   }
```


BqTail function does not wait for Big Query job completion, but instead if generate deferred actions JSON file with job ID
to ${config.DeferTaskURL}/tasks/${JobID}.json 

Once BigQuery job completes, Dispatch Service is notified with completed BigQuery job ID, to pick deferred action execution. 


**Note:**

BigQuery load time does not affect post action execution, since it is delegated to BqDispatch once LoadJob is completed.


### BqTail

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**:
    - [gs://${config.Bucket}/data/case004/dummy1.json](data/dummy1.json)
    - [gs://${config.Bucket}/data/case004/dummy2.json](data/dummy2.json)

#### Output

* **Batch window transient files**

- gs://${config.Bucket}/batch/dummy/

    - [${eventID1}.tnf](data/expect/batch/eventID1.tnf) 
    - [${eventID2}.tnf](data/expect/batch/eventID2.tnf)
    - [$WindowEndTimestamp.win](data/expect/batch/ts.win)

* **Defer task**:
  - [gs://${config.Bucket}/tasks/${jobID}.json](data/expect/tasks/dispatch.json)

Where jobID is created using the following:

$jobID: ${table}/${storageEventID}/dispatch


* **Data:**
Big Query destination table:

```sql
SELECT * FROM bqtail.dummy
```
 
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
  