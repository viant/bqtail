### Client side batch 

### Scenario:

This scenario tests client side bqtail batch ingestion with rule creates on the fly

```
 export GOOGLE_APPLICATION_CREDENTIALS='${env.HOME}/.secret/${gcpCredentials}.json'
 bqtail -s='${parent.path}/data/' -r=rule/rule.yaml 

```



[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case037
  Suffix: .json
Dest:
  Table: bqtail.dummy_v037
  DMLAppend: true
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

