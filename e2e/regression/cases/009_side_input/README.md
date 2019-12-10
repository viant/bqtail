### Data transformation with side input

### Scenario:

This scenario test a transformation expressions with sied input defined below:


[@rule.json](rule.json)
```json
[
  {
    "When": {
      "Prefix": "/data/case009",
      "Suffix": ".json"
    },
    "Async": true,
    "Dest": {
      "Table": "bqtail.dummy",
      "TransientDataset": "temp",
      "Transform": {
        "event_type": "et.name"
      },
      "SideInputs": [
        {
          "Table": "bqtail.event_types",
          "Alias": "et",
          "On": "t.type_id = et.id"
        }
      ]
    },
    "OnSuccess": [
      {
        "Action": "delete"
      }
    ]
  }
]
```