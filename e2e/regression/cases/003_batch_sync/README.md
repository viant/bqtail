### Synchronous in batch data ingestion

### Scenario:

This scenario tests batched synchronous data ingestion, with delete post actions.


BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case003/dummy[1..2].json
It matches the the following rule data ingestion rule.


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case${parentIndex}/
  Suffix: .json
Dest:
  Table: bqtail.dummy_v${parentIndex}
Batch:
  RollOver: true
  Window:
    DurationInSec: 15
OnSuccess:
  - Action: delete
```

**Note:**

When BigQuery load time takes more than max cloud function execution time, the function is terminated, but BigQuery job continues.
Consider using async mode to both reduce CF cost and efficiency.


### BqTail

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**:
    - [gs://${triggerBucket}/data/case003/dummy1.json](data/trigger/dummy1.json)
    - [gs://${triggerBucket}/data/case003/dummy2.json](data/trigger/dummy2.json)
* ${BatchURL}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win


#### Output

* **Data:**
    Big Query destination table:

    ```sql
    SELECT * FROM bqtail.dummy
    ```
 
* **Batch window transient files**

 - ${BatchURL}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win


* **Logs:** 
  - ${JournalURL}/Running/bqtail.dummy/$Date/${eventID}.run
    
* **Stack driver**
  - Response status