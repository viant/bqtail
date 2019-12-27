### Ingestion with transient dataset 

### Scenario:

This scenario tests data deduplication with nested table schema.


BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case007/dummy[1-2].json
It matches the the following rule, to ingest data with transient table in temp dataset,  then it deduplicate data in the destination table.



```json
{
      "When": {
        "Prefix": "/data/case007",
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
          "DurationInSec": 10
        }
      }
}
```

Since table uses nested column the following SQL is used for de duplications

```sql
SELECT id, type_id, name, refs
FROM (
  SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id) row_number
  FROM temp.dummy_741874428359613
)
WHERE row_number = 1
```

* **Batch window transient file**
    - ${BatchURL}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win

* **Logs:** 
    - ${JournalURL}/Done/bqtail.dummy/$Date/${eventID}.run


### BqDispatch

### Input

* **Big Query job status**
* **Batch window transient files**

    - ${BatchURL}/${destTable}_${sourcePathHash}_${dueRunUnixTimestamp}.win

* **Logs:** 
    - ${JournalURL}/Running/bqtail.dummy/$Date/${eventID}.run


### Output

* Trigger 
  - gs://${triggerBucket}/_batch_
  - gs://${triggerBucket}/_bqJob_

* **Stack driver**
  - Response status
  