### Client side batch 

### Scenario:

This scenario tests client side bqtail with separate transient and dest template schema.
While transient schema has encoded_segments STRING column, dest schema has segement ARRAY<STRING>.
The rule uses  encoded_segments to segments transformation.

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
  Table: bqtail.dummy_v041
  Schema:
    Template: bqtail.dummy_v041_tmpl
  Transient:
    Alias: t
    Template: bqtail.dummy_v041_temp_tmpl
    Dataset: temp
  Transform:
    segments: SPLIT(t.encoded_segments, ",")

  Async: true
OnSuccess:
  - Action: delete
```