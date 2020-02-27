### Client side batch 

### Scenario:

This scenario tests client side bqtail batch ingestion with rule creates on the fly

```
  export GOOGLE_APPLICATION_CREDENTIALS='${env.HOME}/.secret/${gcpCredentials}.json'
- ${bqtailCmd} -s='${parent.path}/data/trigger' -d='bqtail.dummy_v036' -w=15

```

