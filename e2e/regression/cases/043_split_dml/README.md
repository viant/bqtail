### Dynamic destination mapping

### Scenario:

This scenario tests destination table mapping based on source data values with DML append.


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case043/
  Suffix: .json
Async: true
Dest:
  Table: bqtail.dummy_v043
  Transient:
    Dataset: temp
    CopyMethod: DML

  Schema:
    Template: bqtail.dummy_v043
    Split:
      ClusterColumns:
        - id
      Mapping:
        - When: MOD(id, 2) = 0
          Then: bqtail.dummy_v043_0
        - When: MOD(id, 2) = 1
          Then: bqtail.dummy_v043_1
OnSuccess:
  - Action: delete


```
