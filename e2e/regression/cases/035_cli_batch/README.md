### Client side batch 

### Scenario:

This scenario tests client side bqtail with batch ingestion rule.

```bash
  export GOOGLE_APPLICATION_CREDENTIALS='${env.HOME}/.secret/${gcpCredentials}.json'
  bqtail -r='${parent.path}/rule/rule.yaml' -s='${parent.path}/data/trigger'
```


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case034
  Suffix: .json
Dest:
  Table: bqtail.dummy_034
Batch:
  Window:
    DurationInSec: 15
Async: true
OnSuccess:
  - Action: delete
```