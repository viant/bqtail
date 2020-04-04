# Dispatch Service

BqDispatch service is responsible for
 - triggering completed Big Query jobs that have post actions
 - triggering scheduled batch load jobs that are due to run


BqDispatcher never submits Big Query job, but detect job completion submitted by BqTail by checking job status.
When BqTail submit BiqQuery job, it create a trace file in AsyncTaskURL, with success or failure post action tasks.
Once BigQuery job is done, BqDispatcher move a trace file to BqTail Trigger bucket.

Dispatch service is a HTTP cloud function that is scheduled to run every minutes.

During each run dispatcher matches scheduled batched load jobs and completed Big Query job with with post action file [${AsyncTaskURL}/${JobID}.json]
Where $JobID uses [info.go](../../../../stage/info.go) to encode dest table, original data trigger EventID, step, and actions
```json
{
  "Status": "ok",
  "Jobs": {},
  "Batched": {},
  "BatchCount": 0,
  "Cycles": 12,
  "Errors": [],
  "Running": {
    "QueryJobs": 1,
    "LoadJobs": 5
  },
  "Pending": {},
  "Dispatched": {
    "QueryJobs": 40,
    "LoadJobs": 10
  },
  "Throttled": {}
}
```

### Configuration:


Configuration is defined as [config.go](config.go)

**Configuration options:**

- JournalURL: active/past job journal URL 
- ErrorURL: - errors location
- AsyncTaskURL: transient storage location for managing async batches and BigQuery job post actions 
- TriggerBucket - trigger bucket
- CheckInMs reload config changes frequency
- TimeToLiveInMin: time to live in sec (1 second by default)
- MaxConcurrentSQL: if specified control number of dispatched SQL events
- MaxConcurrentJobs: if specified control number of dispatched Load/Copy events
     Note that there is undocumented Big Query quota of 20 concurrent load/export jobs, affecting load performance till quota is cleared (hourly).  


Example configuration

[@config.json](usage/dispatch.json)
```json
{
  "ErrorURL": "gs://${opsBucket}/BqDispatch/errors/",
  "JournalURL": "gs://${opsBucket}/BqDispatch/Journal/",
  "AsyncTaskURL": "gs://${dispatchBucket}/BqDispatch/Tasks/",
  "CheckInMs": 10,
  "TimeToLiveInMin": 1,
  "TriggerBucket": "${triggerBucket}",
  "SlackCredentials": {
    "URL": "gs://${configBucket}/Secrets/slack.json.enc",
    "Key": "${prefix}_ring/${prefix}_key"
  },
  "MaxConcurrentSQL": 50,
  "MaxConcurrentJobs": 20
}
```


### Deployment

See [Generic Deployment](../deployment/README.md) automation and post deployment testing  