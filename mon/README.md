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
curl/wget  https://${region}-${ProjectID}.cloudfunctions.net/BqMonitor?IncludeDone=true&DestBucket=${bqTailTirggerBucket}&DestPath=bqmon
```
where:
 - IncludeDone: optional flag to include recently done task (may increase execution time) 
 - DestBucket: optional Google Storage bucket to store service response 
 - DestPath: optional Google Storage path to store service response
 

### Analyzing monitoring status 

Store response of monitoring request in BigQuery with simple bqtail rule:

[@rule.yaml](../deployment/monitor/rule.yaml)
```yaml
When:
  Prefix: "/bqmon/"
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
```sql
CREATE OR REPLACE  TABLE bqtail.bqmon (
    Timestamp TIMESTAMP,
    Status STRING,
    Error  STRING,
    UploadError STRING,
    PermissionError STRING,
    SchemaError STRING,
    CorruptedError STRING,
    Running STRUCT<
                   Count INT64,
                   Min TIMESTAMP,
                   Max TIMESTAMP,
                   Lag STRING,
                   LagInSec INT64
                 >,
    Stages STRUCT<
                        Items  ARRAY<STRUCT<
                                            Key STRING,
                                            Min TIMESTAMP,
                                            Max TIMESTAMP,
                                            Count INT64
                                        >
                                >
    >,
    Stalled STRUCT<
                    Min TIMESTAMP,
                    Max TIMESTAMP,
                    Count INT64,
                    Lag STRING,
                    LagInSec INT64
    >,
    Scheduled STRUCT<
                   Count INT64,
                   Min TIMESTAMP,
                   Max TIMESTAMP,
                   Lag STRING,
                   LagInSec INT64
    >,
   InvalidSchema STRUCT<
        Min TIMESTAMP,
        Max TIMESTAMP,
        Count INT64
    >,
    Corrupted STRUCT<
        Min TIMESTAMP,
        Max TIMESTAMP,
        Count INT64
    >,
    Dest ARRAY<
            STRUCT<
                    Table STRING,
                    RuleURL STRING,
                    Running STRUCT<
                                   Count INT64,
                                   Min TIMESTAMP,
                                   Max TIMESTAMP,
                                   Lag STRING,
                                   LagInSec INT64
                                 >,
                    Scheduled STRUCT<
                                   Count INT64,
                                   Min TIMESTAMP,
                                   Max TIMESTAMP,
                                   Lag STRING,
                                   LagInSec INT64
                    >,
                    Done STRUCT<
                            Min TIMESTAMP,
                            Max TIMESTAMP,
                            Count INT64
                    >,
                    Stalled STRUCT<
                        Min TIMESTAMP,
                        Max TIMESTAMP,
                        Count INT64,
                        Lag STRING,
                        LagInSec INT64
                    >,
                    Stages STRUCT<
                        Items  ARRAY<STRUCT<
                                            Key STRING,
                                            Min TIMESTAMP,
                                            Max TIMESTAMP,
                                            Count INT64
                                        >
                                >
                    >,
                    Error STRUCT<
                                 ProcessURL STRING,
                                 Message STRING,
                                 EventID INT64,
                                 ModTime TIMESTAMP,
                                 Destination STRING,
                                 IsPermission BOOL,
                                 IsSchema BOOL,
                                 IsCorrupted BOOL
                    >,
                    InvalidSchema STRUCT<
                                        Min TIMESTAMP,
                                        Max TIMESTAMP,
                                        Count INT64
                    >,
                    Corrupted STRUCT<
                            Min TIMESTAMP,
                            Max TIMESTAMP,
                            Count INT64
                    >
            >
        >
) PARTITION BY DATE(Timestamp);
```
 
 
### Deployment 

Monitoring service can be run manually or can be scheduled with cloud scheduler.

The following [link](../deployment/README.md#monitoring) details monitoring deployment.
