### Data copy trigger by Query with destination 

### Scenario:

This scenario tests data copy triggered  by loading data to a target table.

BqDispatch function is notified with all Big Query jobs completion, it matches actions to run
with the following rule to export destination table to google storage gs://${config.Bucket}/export/dummy.json.gz
 

[@rule.json](rule.json)
```json
[{
      "When": {
        "Dest": ".+:bqdispatch\\.dummy_v3",
        "Type": "LOAD"
      },
      "OnSuccess": [
        {
          "Action": "copy",
          "Request": {
            "Dest": "bqdispatch.dummy_v4"
          }
        }
      ]
}]
```



### BqDispatch


#### Input:

* **Trigger**:
  - eventType: google.cloud.bigquery.job.complete
  - resource: projects/${projectID}/jobs/{jobId}
* **Configuration:** [gs://e2e-data/config/bqdispatch.json](../../../config/bqdispatch.json)

* **Data**:

Big Query Load Job with destination bqdispatch.dummy_v3  table. 

#### Output

* **Logs:** 

- ${JournalURL}/

* **Data:**
- gs://${config.Bucket}/export/dummy.json.gz
