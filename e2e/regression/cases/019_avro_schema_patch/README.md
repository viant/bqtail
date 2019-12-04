### Synchronous data files ingestion

### Scenario:

This scenario tests on failure action execution.

Table wrong_dummy does not exists thus data load fails.

BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case012/dummy.json
It matches the the following rule to submit load Job to BiqQuery that fails.
On failure actions run with slack notification.


[@rule.json](rule.json)
```json
[{
  "When": {
    "Prefix": "/data/case012",
    "Suffix": ".json"
  },
  "Dest": {
    "Table": "bqtail.wrong_dummy"
  },
  "OnFailure": [
    {
      "Action": "notify",
      "Request": {
        "Channels": [
          "#e2e"
        ],
        "From": "BqTail",
        "Title": "bqtail.wrong_dummy ingestion",
        "Message": "$Error",
        "Secret": {
          "URL": "gs://${config.Bucket}/config/slack.json.enc",
          "Key": "BqTailRing/BqTailKey"
        }
      }
    }
  ]
}]
```

#### Input:

* **Trigger**:
    - eventType: google.storage.object.finalize
    - resource: projects/_/buckets/${config.Bucket}
* **Configuration:** [gs://e2e-data/config/bqtail.json](../../../config/bqtail.json)
* **Data**: [gs://${triggerBucket}/data/case001/dummy.json](data/trigger/dummy.json)

#### Output
 
* **Logs:** 

- [${JournalURL}/dummy/${date}/${storageEventId}/tail-job.json](data/expect/journal/tail-job.json)

* **Data:**
  message on the #e2e slack channel
