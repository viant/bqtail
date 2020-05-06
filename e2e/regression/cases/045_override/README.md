### Client side batch 

### Scenario:

This scenario tests client side bqtail with batch ingestion rule, with override attribute 

```bash
  export GOOGLE_APPLICATION_CREDENTIALS='${env.HOME}/.secret/${gcpCredentials}.json'
  bqtail -r='${parent.path}/rule/rule.yaml' -s='${parent.path}/data/trigger'
```


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case045
  Suffix: .json
Dest:
  Table: bqtail.dummy_v045
  Override: true
  Transient:
    Dataset: temp

Batch:
  MultiPath: true
  Window:
    DurationInSec: 15
Async: true
OnSuccess:
  - Action: delete

```