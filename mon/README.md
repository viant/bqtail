# Monitoring service

Monitoring service uses Google Storage BqTail operation files to determine current processing status.

- Running load processes can be found in $config.ActiveLoadProcessURL
- Done load processes can be found in $config.DoneLoadProcessURL
- All processing stages file can be found in $config.AsyncTaskURL 
- All errors can be found in $config.ErrorURL


Each load process creates a run file with all instruction load in $config.ActiveLoadProcessURL (default $config.JournalURL/Running)
which is moved to $DoneLoadProcessURL location as the last step of load process.

If something goes wrong in between the process run file can:
 - be replayed in case of internal server error or backend error
 - moved to $ErrorURL in case of unrecoverable error.
 - stay forever in ActiveLoadProcessURL (unhandled case - should never happen)                                                                          	
 
 
## Usage

 ```bash
curl/wget  https://${region}-${ProjectID}.cloudfunctions.net/BqMonitor?IncludeDone=true&DestBucket=${bqTailTirggerBucket}&DestPath=sys/bqmon
```
where:
 - IncludeDone: optional flag to include recently done task (may increase execution time) 
 - DestBucket: optional Google Storage bucket to store service response 
 - DestPath: optional Google Storage path to store service response
 

### Analyzing monitoring status 

Store response of monitoring request in BigQuery with simple bqtail rule:

[@rule.yaml](../deployment/monitor/rule/bqmon.yaml)
```yaml
When:
  Prefix: "/sys/bqmon/"
  Suffix: ".json"
Async: true
Dest:
  Table: bqtail.bqmon
Batch:
  Window:
    DurationInSec: 120
OnSuccess:
  - Action: delete

```

Make sure that destination table uses the following schema:

[@schema.sql](schema/schema.sql)
 
Once all is in place you can build query base monitoring with the following:

```sql
SELECT 
    Status, 
    Timestamp, 
    Running.Count AS Runnng,
    Running.LagInSec AS RunningLagInSec,
    Scheduled.Count AS Scheduled,
    (SELECT SUM(i.Count) as stalled FROM UNNEST(Stalled.Items) i) AS Stalled,
    Error, 
    PermissionError,
    SchemaError, 
    CorruptedError
FROM `bqtail.bqmon`
WHERE DATE(timestamp) = CURRENT_DATE()
ORDER BY timestamp DESC
LIMIT 1
``` 


### Deployment 

Monitoring service can be run manually or can be scheduled with cloud scheduler.

The following [link](../deployment/README.md#monitoring) details monitoring deployment.


 