### Asynchronous data ingestion

### Scenario:

This scenario tests individual asynchronous data ingestion, with delete post actions.

BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case002/dummy[1..2].json
It matches the the following rule data ingestion rule. 

Later it is also notified by BqDispatcher (/${BqJobPrefix}/) once BigQuery job is completed with post actions.

[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case${parentIndex}/"
  Suffix: ".json"
Async: true
Dest:
  Table: bqtail.dummy_v${parentIndex}
OnSuccess:
  - Action: delete

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
    - [gs://${triggerBucket}/data/case002/dummy1.json](data/trigger/dummy1.json)
    - [gs://${triggerBucket}/data/case002/dummy2.json](data/trigger/dummy2.json)

* **Post load tasks transient file**    
    - gs://${triggerBucket}/_bqtail_/${JobID}.json


#### Output

* **Data:**
Big Query destination table:

```sql
SELECT * FROM bqtail.dummy
```

* **Defer task**:
  - [${AsyncTaskURL}/${JobID}.json]

Where $JobID uses [info.go](../../../../stage/info.go) to encode dest table, original data trigger EventID, step, and actions


### BqDispatch

### Input

* **Big Query job status**

* **Post load tasks transient file**
  - [${AsyncTaskURL}/${JobID}.json]
  
  

### Output

* **Post load tasks transient file**    
    - gs://${triggerBucket}/_bqtail_/${JobID}.json

* **Logs:** 
  - ${JournalURL}/Running/bqtail.dummy/$Date/${eventID}.run
    
* **Stack driver**
  - Response status