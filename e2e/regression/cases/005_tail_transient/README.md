### Ingestion with transient dataset in sync mode

### Scenario:

BqTail function is notified once data is upload to gs://${config.Bucket}/data/case005/dummy.json
It matches the the following route, to ingest data with transient table in temp dataset, followed by final destination ingestion.


```json
  {
      "When": {
        "Prefix": "/data/case005",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "bqtail.dummy",
        "TransientDataset": "temp"
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ]
    }
```

Since route is configured in asynchronous mode (default), all post action inherit that mode.
If there is no error temp table is dropped after appending data to dest table.
Name of transient table uses event ID as suffix.


### BqTail

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**:
    - [gs://${config.Bucket}/data/case005/dummy.json](data/dummy.json)

#### Output


* **Data:**

    - temporary loaded data in temp.dummy_${EventID}
    - data copied from  temp.dummy_${EventID} to bqtail.dummy

All SQL sub task moving data from temp to destination can be found in tail.job.json journal. 
 
* **Logs:** 

    - [gs://${config.Bucket}/journal/dummy_${eventID}/${date}/${EventID}/tail-job.json](data/expect/journal/tail-job.json)

### BqDispatch

N/A
