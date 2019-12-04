# Dispatch Service

BqDispatch service is responsible for
 - triggering completed Big Query jobs that have post actions
 - triggering scheduled batch load jobs that are due 


Dispatch service is a HTTP cloud function that is scheduled to run every minutes.

During each run dispatcher matches scheduled batched load jobs and completed Big Query job with with post action file [${AsyncTaskURL}/${JobID}.json]
Where $JobID uses [info.go](../../../../stage/info.go) to encode dest table, original data trigger EventID, step, and actions
```json
{
  "Status": "ok",
  "Jobs": {
    "...":"..." 
  },
  "Batched": {
    "...":"..."
  },
  "BatchCount": 44,
  "Cycles": 4,
  "ListTime": "3.71546899s",
  "ListCount": 573,
  "GetCount": 49,
  "Errors": [],
  "RunningCount": 20,
  "PendingCount": 0
}
```

### Deployment

See [Generic Deployment](../deployment/README.md) automation and post deployment testing  