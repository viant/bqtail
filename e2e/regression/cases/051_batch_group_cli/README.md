### Batch group ingestion

In case when you need to group all subsequent batched ingestion unless there is no more data arrival, 
you can use batch group. $GroupID variable can be used in table name.


### Scenario:

This scenario tests Group.OnDone action after all batches have been processes.

[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case051/
  Suffix: .json
Dest:
  Table: bqtail.dummy_$GroupID
  Schema:
    Template: bqtail.dummy_v051
  Transient:
    Dataset: temp

Batch:
  RollOver: true
  MultiPath: true
  Window:
    DurationInSec: 15
  Group:
    OnDone:
      - Action: copy
        Request:
          Append: true
          Source: bqtail.dummy_$GroupID
          Dest: bqtail.dummy_v051


Async: true
OnSuccess:
  - Action: delete

```
