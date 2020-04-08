### Client side batch 

### Scenario:

This scenario tests client side bqtail with rule that extract fileName as table name.


```bash
export GOOGLE_APPLICATION_CREDENTIALS='${env.HOME}/.secret/${gcpCredentials}.json'
bqtail -r=rule/rule.yaml -s=data/trigger/
```


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case040
  Suffix: .json
Dest:
  Schema:
    Template: bqtail.dummy_v040
  Table: bqtail.${fileName}
  Pattern: /data/case040/([\w]+)
  Parameters:
    - Expression: $1
      Name: fileName
Async: true
OnSuccess:
  - Action: delete
```