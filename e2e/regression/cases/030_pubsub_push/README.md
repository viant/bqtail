### Data files ingestion with pusbub notification

### Scenario:

This scenario tests individual async data ingestion, with push message to pubsub topic


[@rule.yaml](rule/rule.yaml)
```yaml
Async: true
When:
  Prefix: "/data/case${parentIndex}/"
  Suffix: ".json"
Dest:
  Table: bqtail.dummy_v${parentIndex}
OnSuccess:
  - Action: push
    Request:
      Topic: ${prefix}_bqtailbus
      Attributes:
        EventID: $EventID
        CaseNo: '${parentIndex}'
      Message:
        RuleURL: $RuleURL
        SourceURIs: $LoadURIs
        URLs: $URLs

  - Action: delete
```

