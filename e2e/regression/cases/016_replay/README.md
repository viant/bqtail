### Synchronous data files ingestion

### Scenario:

This scenario tests reapplying archived run file.
New event is used for data processing, and data is ingested as long original data files are present.


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
        "From": "bqtail",
        "Title": "bqtail.wrong_dummy ingestion",
        "Message": "$Error",
        "Secret": {
          "URL": "gs://${config.Bucket}/config/slack.json.enc",
          "Key": "bqtailRing/BqTailKey"
        }
      }
    }
  ]
}]
```
