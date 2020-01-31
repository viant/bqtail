### Data transformation with side input

### Scenario:

This scenario test a transformation expressions with sied input defined below:


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case${parentIndex}/
  Suffix: .json
Dest:
  Table: bqtail.dummy_v${parentIndex}
  Transient:
    Dataset: temp
  TransientAlias: t
  Transform:
    event_type: et.name
  SideInputs:
    - Table: bqtail.event_types
      Alias: et
      On: t.type_id = et.id
Async: true
OnSuccess:
  - Action: delete
```
