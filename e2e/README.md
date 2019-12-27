# End to end testing

## Prerequisites

This project uses [endly](https://github.com/viant/endly/) end to end test runner.

1. Install latest [runner](https://github.com/viant/endly/releases) or use [endly docker image](https://github.com/viant/endly/tree/master/docker)
2. Create dedicated GCP project for  end to end testing.
3. Create e2e service account with admin permission on e2e test project
4. Generate and download [google secrets](https://github.com/viant/endly/tree/master/doc/secrets#gc) to ~/.secret/gcp-e2e.json
5. Create slack OAuth token and store in the ~/secret/slack-e2e.json in the following format:

```json
{
  "Token": "MY_OAUTH_SLACK_TOKEN"
}
```

If you do not have slack testing token just create a file with dummy data.

 
6. Checkout the this project:
```bash
git clone https://bqtail.git
cd bqtail/e2e
```
6. Update Bucket name in config file (it has to be globally unique)
[config/config.json](config/config.json)



## Use cases

To run all test use the following command:

```bash
endly run.yaml
```

To run individual use cases run first init task to upload configs, and deploy cloud functions, followed by individual case run.

```json
endly -t=init
```

- [Synchronous data files ingestionn](regression/cases/001_individual_sync)

```bash
    endly -t=test -i=individual_sync
```


- [Asynchronous data ingestion](regression/cases/002_individual_async)

```bash
    endly -t=test -i=individual_async
```

- [Synchronous in batch data ingestion](regression/cases/003_batch_sync/README.md)

```bash
    endly -t=test -i=batch_sync
```

- [Asynchronous in batch data ingestion](regression/cases/004_batch_async/README.md)

```bash
    endly -t=test -i=batch_async
```

- [Ingestion with transient dataset](regression/cases/005_transient/README.md)

```bash
    endly -t=test -i=transient
```

- [Ingestion with data deduplication](regression/cases/006_batch_dedupe/README.md)

```bash
    endly -t=test -i=batch_dedupe
```

- [Ingestion with nested data deduplication](regression/cases/007_dedupe_nested/README.md)

```bash
    endly -t=test -i=dedupe_nested
```


- [Ingestion with transformation](regression/cases/008_transform)

```bash
    endly -t=test -i=transform
```


- [Ingestion with side input transformation](regression/cases/009_side_input)

```bash
    endly -t=test -i=side_input
```

- [Batch summary SQL task](regression/cases/010_query_task)

```bash
    endly -t=test -i=query_task
```

- [Batch summary SQL task in async mode](regression/cases/011_async_query_task)

```bash
    endly -t=test -i=async_query_task
```


- [Error notification with slack](regression/cases/012_slack_notification)

```bash
    endly -t=test -i=slack_notification
```


- [Dynamic dest table name base on source data](regression/cases/013_schema_split)

```bash
    endly -t=test -i=schema_split
```

- [Corrupted batch recovery](regression/cases/014_batch_with_corruption)

```bash
    endly -t=test -i=batch_with_corruption
```

- [Batch allocation 2k stress test](regression/cases/015_batch_stress)

```bash
    endly -t=test -i=batch_stress
```


- [Ingestion replay](regression/cases/016_replay)

```bash
    endly -t=test -i=replay
```

- [Corrupted batch recovery in sync mode](regression/cases/017_sync_corruption)

```bash
    endly -t=test -i=sync_corruption
```

- [Invalid schema batch recovery](regression/cases/018_invalid_schema)

```bash
    endly -t=test -i=invalid_schema
```

- [Transient Schema](regression/cases/019_transient_schema)

```bash
    endly -t=test -i=transient_schema
```

- [HTTP API Call](regression/cases/020_api_call)

```bash
    endly -t=test -i=api_call
```

- [Data aggregation with side input](regression/cases/021_aggregation)

```bash
    endly -t=test -i=aggregation
```

- [Data aggregation with inline side input](regression/cases/022_aggregation_inline)

```bash
    endly -t=test -i=aggregation_inline
```

