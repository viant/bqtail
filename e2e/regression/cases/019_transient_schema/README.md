### Transient table schema

### Scenario:

This scenario tests transient table schema.

Data ingestion uses CSV file with 3 columns while destination table has 4 columns,
transient template is used to load 3 columnar CSV, followed by final copy data from temp to destination table.


[@rule.json](rule.json)
```json
[
  {
    "When": {
      "Prefix": "/data/case019",
      "Suffix": ".csv"
    },
    "Dest": {
      "Table": "bqtail.dummy",
      "Transient":{
         "Dataset": "temp",
        "Template" : "bqtail.dummy_temp"
      }
    },
    "Batch": {
      "RollOver": true,
      "Window": {
        "DurationInSec": 15
      }
    },
    "Async": true,
    "OnSuccess": [
      {
        "Action": "delete"
      }
    ]
  }
]
```

