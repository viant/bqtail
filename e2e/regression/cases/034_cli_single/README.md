### Client side batch 

### Scenario:

This scenario tests client side bqtail with individual file ingestion.

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
  Table: bqtail.dummy_034
Async: true
OnSuccess:
  - Action: delete

```