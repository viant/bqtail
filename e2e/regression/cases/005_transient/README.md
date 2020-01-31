### Ingestion with transient dataset in sync mode

### Scenario:

This scenario tests data usage of transient/temp dataset and table.


BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case005/dummy.json
It matches the the following rule, to ingest data with transient table in temp dataset, followed by final destination ingestion.



[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case${parentIndex}/
  Suffix: .json
Dest:
  Table: bqtail.dummy_v${parentIndex}
  Transient:
    Dataset: temp
OnSuccess:
  - Action: delete
```


Since rule is configured in asynchronous mode, all post actions inherit that mode.
If there is no error temp table is dropped after appending data to dest table.
Name of transient table uses event ID as suffix.


### BqTail

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**:
    - [gs://${triggerBucket}/data/case005/dummy.json](data/trigger/dummy.json)

#### Output

* **Data:**

    - temporary loaded data in temp.dummy_${EventID}
    - data copied from  temp.dummy_${EventID} to bqtail.dummy (using COPY append API)

 
* **Logs:** 
  - ${JournalURL}/Running/bqtail.dummy/$Date/${eventID}.run
    
* **Stack driver**
  - Response status


### BqDispatch

N/A
