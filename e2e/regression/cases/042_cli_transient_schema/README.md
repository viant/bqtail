### Client side batch 

### Scenario:

This scenario tests client side bqtail with separate transient and dest template schema.
Since this rule has no transformation, and transient schema is not compatible, encoded_segments
information is lost when coping data to the destination table.

```bash
export GOOGLE_APPLICATION_CREDENTIALS='${env.HOME}/.secret/${gcpCredentials}.json'
bqtail -r=rule/rule.yaml -s=data/trigger/dummy.json
```


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case034
  Suffix: .json
Dest:
  Table: bqtail.dummy_v042
  Schema:
    Template: bqtail.dummy_v042_tmpl
  Transient:
    Alias: t
    Template: bqtail.dummy_v042_temp_tmpl
    Dataset: temp

  Async: true
OnSuccess:
  - Action: delete
```