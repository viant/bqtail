### Load access error 

### Scenario:

This scenario test load access error.
The destination is public dataset with no permission  to write, 
Data is first ingested to temp table followed by destination copy which has no write permission.

The validation uses monitoring service, it expect 'PermissionError' attribute set, with status 'ok'


[@rule.json](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case023"
  Suffix: ".json"
Async: true
Batch:
  Window:
    DurationInSec: 15
Dest:
  Table: nyc-tlc:green.trips_2014
  Transient:
    Dataset: temp
OnSuccess:
  - Action: delete

```

