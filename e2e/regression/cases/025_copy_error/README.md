### Copy job error

### Scenario:

This scenario tests a copy job error, temp table and destination have incompatible schema thus copy job result in 'Provided Schema does not match' error.



[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case025"
  Suffix: ".csv"
Dest:
  Table: bqtail.dummy_v25
  Transient:
    Dataset: temp
    Template: bqtail.dummy_v25_temp
Batch:
  RollOver: true
  Window:
    DurationInSec: 10
Async: true
OnSuccess:
  - Action: delete

```

