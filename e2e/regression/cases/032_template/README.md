### Templates

### Scenario:

This scenario test transient and destination table templates to manges schema incompatibility.


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: "/data/case${parentIndex}/"
  Suffix: ".json"
Dest:
  Transient:
    Dataset: temp
    Template: bqtail.dummy_v${parentIndex}_tmpl
  Schema:
    Template: bqtail.dummy_v${parentIndex}
  Pattern: /data/case(\d+)/(\d{4})/(\d{2})/(\d{2})/
  Table: bqtail.dummy_v${parentIndex}_$MyTableSufix
  Parameters:
    - Name: MyTableSufix
      Expression: $2$3$4
    - Name: MyDate
      Expression: $2-$3-$4
    - Name: CaseNo
      Expression: '$1'
  Transform:
    date: DATE('$MyDate')
    use_case: "'$CaseNo'"

OnSuccess:
  - Action: delete


```

