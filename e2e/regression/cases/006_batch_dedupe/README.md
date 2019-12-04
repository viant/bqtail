### Ingestion with transient dataset deduplication in async mode  

### Scenario:

This scenario tests data deduplication with basic table schema.


BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case006/dummy[1-2].json
It matches the the following rule, to ingest data with transient table in temp dataset, followed by running deduplication SQL to the destination table.

Later BqTail is also notified by BqDispatcher with batch which is due to run (/${BatchPrefix}/)  
and corresponding (/${BqJobPrefix}/) BigQuery job that have post actions. 


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

**Note**

When transient dataset is used,  once load job is completed data is moved to destination table using Copy with append job as long there is no data deduplication or transformation.
While copy operation is free, using deduplication or data transformation double cost. 

### BqTail

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**:
    - [gs://${triggerBucket}/data/case006/dummy1.json](data/trigger/dummy1.json)
    - [gs://${triggerBucket}/data/case006/dummy2.json](data/trigger/dummy2.json)

* **Batch load tasks transient file**    
    - gs://${triggerBucket}/${BatchPrefix}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win

* **Post load tasks transient file**    
    - gs://${triggerBucket}/${BqJobPrefix}/${JobID}.json


#### Output

* **Data:**
Big Query temp table:

```sql
SELECT * FROM temp.dummy
```

* **Batch window transient files**

    - ${BatchURL}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win

* **Logs:** 
    - ${JournalURL}/Running/bqtail.dummy/$Date/${eventID}.run

* **Stack driver**
    - Response status

* **Post load tasks transient file**    
    - ${AsyncTaskURL}/${JobID}.json

    
### BqDispatch

### Input

* **Big Query job status**

* **Batch window transient files**
    - ${BatchURL}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win
    
* **Post load tasks transient file**    
    - ${AsyncTaskURL}/${JobID}.json

 ### Output

* **Logs:** 
    - ${JournalURL}/Running/bqtail.dummy/$Date/${eventID}.run
    
* **Stack driver**
    - Response status
 
* **Post load tasks transient file**    
    - gs://${triggerBucket}/${BqJobPrefix}/${JobID}.json
  
