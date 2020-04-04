# End to end testing

## Prerequisites

This project uses [endly](https://github.com/viant/endly/) end to end test runner.

Min required endly version: 0.47.1

1. Install latest [runner](https://github.com/viant/endly/releases) or use [endly docker image](https://github.com/viant/endly/tree/master/docker)
2. Create dedicated GCP project for  end to end testing.
3. Create e2e service account with admin permission on e2e test project
4. Setup credentials
-  [SSH credentials](https://github.com/viant/endly/tree/master/doc/secrets#ssh)
- [Google Secrets for service account](https://github.com/viant/endly/tree/master/doc/secrets#google-cloud-credentials)
    * Store main test project service account google secrets in  ~/.secret/gcp-e2e.json
    * Optionally for multi project load balancer tests store second test project service account in  ~/.secret/bq-e2e.json
 Optionally create slack OAuth token and store in the ~/secret/slack-e2e.json in the following format:

```json
{
  "Token": "MY_OAUTH_SLACK_TOKEN"
}
```


If you do not have slack testing token just create a file with dummy data.
 
5. Checkout the this project:
```bash
git clone https://github.com/viant/bqtail.git
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


- [Dynamic dest table name base on source data](regression/cases/013_table_split)

```bash
    endly -t=test -i=table_split
```

- [Aggregation ETL](regression/cases/014_aggregation)

```bash
    endly -t=test -i=aggregation
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

- [Inline Aggregation](regression/cases/018_aggregation_inline)

```bash
    endly -t=test -i=aggregation_inline
```

- [Transient Schema](regression/cases/019_transient_schema)

```bash
    endly -t=test -i=transient_schema
```

- [HTTP API Call](regression/cases/020_api_call)

```bash
    endly -t=test -i=api_call
```

- [Partial batch with corruption](regression/cases/021_partial_batch_corruption)

```bash
    endly -t=test -i=partial_batch_corruption
```

- [Partial schema error](regression/cases/022_partial_schema_error)

```bash
    endly -t=test -i=schema_error
```

- [Permission error](regression/cases/023_permission_error)

```bash
    
    #remove skip.txt then run:
    endly -t=test -i=permission_error
```


- [Batch corruption](regression/cases/024_batch_corruption)

```bash
    endly -t=test -i=batch_corruption
```


- [Copy job error](regression/cases/025_copy_error)

```bash
    endly -t=test -i=copy_error
```


- [Post query job error](regression/cases/026_post_query_error)

```bash
    endly -t=test -i=post_query_error
```



- [Explicit Transient Project](regression/cases/027_transient_project)

```bash
    endly -t=test -i=transient_project
```


- [Explicit Projects Loadbalancer](regression/cases/028_project_balancer)

```bash
    endly -t=test -i=project_balancer
```

- [Copy job with explicit Project](regression/cases/029_transient_copy)

```bash
    endly -t=test -i=transient_copy
```

- [Ingestion with PubSub notification](regression/cases/030_pubsub_push)

```bash
    endly -t=test -i=pubsub_push
```


- [Ingestion with URL pattern parameters](regression/cases/031_pattern_params)

```bash
    endly -t=test -i=pattern_params
```

- [Ingestion with tempaltes](regression/cases/032_template)

```bash
    endly -t=test -i=template
```

- [Ingestion with partition_override](regression/cases/033_partition_override)

```bash
    endly -t=test -i=partition_override
```


- [CLI single file ingestion](regression/cases/034_cli_single)

```bash
    endly -t=test -i=cli_single
```

- [CLI batch ingestion](regression/cases/035_cli_batch)

```bash
    endly -t=test -i=cli_batch
```

- [CLI dynamic rule ingestion](regression/cases/036_cli_dynamic_rule)

```bash
    endly -t=test -i=cli_dynamic_rule
```

- [Ingestion with DML append](regression/cases/037_cli_dml_append)

```bash
    endly -t=test -i=cli_dml_append
```

- [Ingestion ALLowFieldAddition for JSON](regression/cases/038_cli_json_field_addition)

```bash
    endly -t=test -i=cli_json_field_addition
```
