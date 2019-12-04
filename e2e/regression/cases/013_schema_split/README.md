### Dynamic destination mapping

### Scenario:

This scenario tests destination table mapping based on source data values.

For cost purpose temp table is partitioned and clustered on the column used in split criteria.

[@rule.json](rule.json)
```json
[
  {
    "When": {
      "Prefix": "/data/case013",
      "Suffix": ".json"
    },
    "Async": true,
    "Dest": {
      "Table": "bqtail.dummy",
      "TransientDataset": "temp",
      "Schema": {
        "Template": "bqtail.dummy",
        "Split": {
          "ClusterColumns": [
            "id", "info.key"
          ],
          "Mapping": [
            {
              "When": "MOD(id, 2) = 0",
              "Then": "bqtail.dummy_0"
            },
            {
              "When": "MOD(id, 2) = 1",
              "Then": "bqtail.dummy_1"
            }
          ]
        }
      }
    },
    "OnSuccess": [
      {
        "Action": "delete"
      }
    ],
    "OnFailure": [
      {
        "Action": "notify",
        "Request": {
          "Channels": [
            "#e2e"
          ],
          "Title": "bqtail.schema split",
          "Message": "$Error"
        }
      }
    ]
  }
]
```
