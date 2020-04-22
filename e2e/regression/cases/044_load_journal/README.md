### Client side batch 

### Scenario:

This scenario tests client side bqtail with batch ingestion rule, with insert action 

```bash
  export GOOGLE_APPLICATION_CREDENTIALS='${env.HOME}/.secret/${gcpCredentials}.json'
  bqtail -r='${parent.path}/rule/rule.yaml' -s='${parent.path}/data/trigger'
```


[@rule.yaml](rule/rule.yaml)
```yaml
When:
  Prefix: /data/case044
  Suffix: .json
Dest:
  Table: bqtail.dummy_v044
  Transient:
    Dataset: temp
Batch:
  MultiPath: true
  Window:
    DurationInSec: 15
Async: true
OnSuccess:
  - Action: delete
  - Action: insert
    Request:
      Dest: bqtail.dummy_v044_journal
      SQL: SELECT '$EventID' AS EventID, SPLIT('$URLs',',') AS URLS, COUNT(1) AS Records, CURRENT_TIMESTAMP() AS Loaded FROM $TempTable

```