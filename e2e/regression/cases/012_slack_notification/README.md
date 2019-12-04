### Synchronous data files ingestion

### Scenario:

This scenario tests on failure action execution.

Table wrong_dummy does not exists thus data load fails.

BqTail function is notified once data is uploaded to gs://${triggerBucket}/data/case012/dummy.json
It matches the the following rule to submit load Job to BiqQuery that fails.
On failure actions run with slack notification.


[@rule.json](rule.json)
```json
[
  {
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
          "Title": "bqtail.wrong_dummy ingestion",
          "Message": "$Error"
        }
      }
    ]
  }
]
```
