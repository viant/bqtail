### Dynamic destination mapping

### Scenario:

This scenario tests destination table mapping based on source data values.

For cost purpose temp table is partitioned and clustered on the column used in split criteria.

[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case${parentIndex}/
  Suffix: .json
Async: true
Dest:
  Table: bqtail.dummy_v${parentIndex}
  TransientDataset: temp
  Schema:
    Template: bqtail.dummy_v${parentIndex}
    Split:
      ClusterColumns:
        - id
        - info.key
      Mapping:
        - When: MOD(id, 2) = 0
          Then: bqtail.dummy_v${parentIndex}_0
        - When: MOD(id, 2) = 1
          Then: bqtail.dummy_v${parentIndex}_1
OnSuccess:
  - Action: delete

```
