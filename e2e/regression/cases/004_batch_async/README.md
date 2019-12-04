### Asynchronous in batch data ingestion

### Scenario:

This scenario tests batched asynchronous data ingestion, with delete post actions.

BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case003/dummy[1..2].json
It matches the the following rule data ingestion rule.

Later BqTail is also notified by BqDispatcher with batch which is due to run (/${BatchPrefix}/)  
and corresponding (/${BqJobPrefix}/) BigQuery job that have post actions. 


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

Each file tries to acquire batch window per destination table. Only one data file can successfully acquire window for specified batch time window.
All other data file result in matching batch owner event ID. 

### BqTail

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**:
    - [gs://${triggerBucket}/data/case004/dummy1.json](data/trigger/dummy1.json)
    - [gs://${triggerBucket}/data/case004/dummy2.json](data/trigger/dummy2.json)


* **Batch load tasks transient file**    
    - gs://${triggerBucket}/${BatchPrefix}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win

* **Post load tasks transient file**    
    - gs://${triggerBucket}/${BqJobPrefix}/${JobID}.json


#### Output
 
* **Batch window transient files**

- ${BatchURL}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win

* **Post load tasks transient file**    
    - ${AsyncTaskURL}/${JobID}

* **Logs:** 
    - ${JournalURL}/Running/bqtail.dummy/$Date/${eventID}.run

* **Stack driver**
    - Response status
    

### BqDispatch

#### Input:

* **Big Query job status**

* **Batch window transient files**

 - ${BatchURL}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win


* **Post load tasks transient file**    
    - ${AsyncTaskURL}/xxx


#### Output


* **Batch window transient files**

- ${BatchURL}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win

* **Post load tasks transient file**    
    - ${AsyncTaskURL}/${JobID}

* **Data:**
    Big Query destination table:

    ```sql
    SELECT * FROM bqtail.dummy
    ```


* **Logs:** 
  - ${JournalURL}/Running/bqtail.dummy/$Date/${eventID}.run
    
* **Stack driver**
  - Response status