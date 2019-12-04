## Tail Service

Tail service ingests data to Big Query with Load API. 

- [Introduction](#introduction)
- [Usage](#usage)
- [Deployment](#deployment)

### Introduction 

![BqTail](../images/tail.png)  


### Usage

### Configuration

Configuration is defined as [config.go](config.go)

The following example defines single rule to ingested any JSON file matching /data/folder prefix to mydataset.mytable.

In this configuration scenario each data file is ingested  to Big Query by dedicated cloud function instance.
After initiating a load Job, the function waits for job completion to finally run on success tasks. 

This per data file synchronous mode is very cost inefficient, since each data file uses dedicated cloud function for the whole data ingestion duration.
Another limitation that needs to be consider is number of data files, if you expect more than 1K per day you reach daily load job count restriction.
On top of that if Big Query load job takes more than cloud function max execution time, the function is terminated 
and the post task execution does not run at all.

All these limitations are addressed by asynchronous and batch mode. 
 
[@config/sync.json](usage/sync.json)
```json
[
    {
      "When": {
        "Prefix": "/data/folder",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "mydataset.mytable"
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ]
    }
]
```

**Configuration options:**

- JournalURL: job history location 
- ErrorURL: - errors location
- AsyncTaskURL: transient storage location for managing BigQuery job post tasks (both BqTail and BqDispatch have to use the same URL) 
- AsyncBatchURL: transient storage location for managing schedule batch load job in async mode.
- BatchURL: transient storage location for managing batch load job in sync mode.
- RulesURL: base URL where each rule is JSON file with rules arrays
- ActiveIngestionURL: currently running data ingestion job URL 
- DoneIngestionURL: completed data ingestion jobs URL
- TriggerBucket: trigger bucket
- CorruptedFileURL: url for currpupted files
- InvalidSchemaURLL: url for incompatible schema files
- SlackCredentials

To reduce Storage Class A operations: cache file is used for config files:  delete cache file alongside adding a new rule.

**Individual rule** can has the following attributes:

- When defines matching filter
  - Prefix: path prefix or
  - Suffix: path suffix or
  - Filter: path regexp
- OnSuccess: actions to run when job completed without errors
- OnFailure: actions to run when job completed with errors

Post actions can use predefined [Cloud Service](../service/README.md) operation.


#### Asynchronous mode

In asynchronous mode BqTailFn cloud function schedules post task execution using deferred task base URL and submit load job. 
Once BigQuery job completes it is picked up by BqDispatchFn cloud function to run post tasks. 

In this mode cloud function execution time is stremlined to actual task run without unnecessary waits.


[@config/async.json](usage/async.json)
```json
 [
    {
      "When": {
        "Prefix": "/data/folder",
        "Suffix": ".json"
      },
      "Async": true,
      "Dest": {
        "Table": "mydataset.mytable"
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ],
      "OnFailure": [
        {
          "Action": "move",
          "Request": {
            "DestURL": "gs://e2e-data/errors"
          }
        }
      ]
    }
]
```

#### Batch ingestion

In batch mode, storage event are batch by time window, in this mode only one cloud function manages batching while others ends
after scheduling event source.  The batching process is details [here](batch/README.md).

Batch URL is used to manged batch windowing process.

The following configuration specify batch sync mode.

[@config/batch.json](usage/batch.json)
```json
[
    {
      "When": {
        "Prefix": "/data/folder",
        "Suffix": ".json"
      },
      "Batch": {
        "Window": {
          "DurationInSec": 45
        }
      },
      "Dest": {
        "Table": "mydataset.mytable"
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ]
    }
  ]
```

The following configuration specify batch asynchronous  mode.

[@config/async_batch.json](usage/async_batch.json)
```json
[
    {
      "When": {
        "Prefix": "/data/folder",
        "Suffix": ".json"
      },
      "Async": true,
      "Batch": {
        "Window": {
          "DurationInSec": 45
        }
      },
      "Dest": {
        "Table": "mydataset.mytable"
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ]
    }
  ]
```


#### Transient Dataset

When ingesting data, from one or many datafiles, some entries may be corrupted impacting data quality.
To add extra data quality check you can use transient dataset. In this case data is moved to destination table
only if temp table data ingestion was successful.

Additional benefits of transient transfer is using dedicated transient project for ingestion only, where BqDispatch only
get ingestion notification. Finally separating transinet and final destination project allows you to better control 
various BigQuery limits lik 1K load jobs per table or 100K load jobs per project.


Temp table is constructed from destination table suffixed by event ID.

The following configuration specify transient dataset.
[@config/transient.json](usage/transient.json)
```json
[

    {
      "When": {
        "Prefix": "/data/folder",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "mydataset.mytable",
        "TransientDataset": "temp"
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ]
    }
]
```

#### Data deduplication

When using transient table you can specify unique columns to deduplicate data while moving to destination table.


[@config/dedupe.json](usage/dedupe.json)
```json
[
  {
      "Async": true,
      "When": {
        "Prefix": "/data/folder",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "mydataset.mytable",
        "TransientDataset": "temp",
        "UniqueColumns": [
          "id"
        ]
      },
      "Batch": {
        "Window": {
          "DurationInSec": 80
        }
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ]
  }
]
```

#### Template table

In case destination table does not exists you can specify schema source table with schema.template attribute.

[@config/template.json](usage/template.json)
```json
[
    {
      "When": {
        "Prefix": "/data/folder",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "mydataset.mytable",
        "Schema": {
          "Template": "mydataset.template_table"
        }
      }
   }
]
```


#### Dynamic table destination based on source data.


To dynamically rule data based on source data values you can use the following rule.

[@config/dynamic_dest.json](usage/dynamic_dest.json)

```json
[
  {
    "When": {
      "Prefix": "/data/case013",
      "Suffix": ".json"
    },
    "Async": true,
    "Dest": {
      "Table": "bqtail.dummy",
      "TransientDataset": "temp",
      "Schema": {
        "Template": "bqtail.dummy",
        "Split": {
          "ClusterColumns": [
            "id",
            "info.key"
          ],
          "Mapping": [
            {
              "When": "MOD(id, 2) = 0",
              "Then": "bqtail.dummy_0"
            },
            {
              "When": "MOD(id, 2) = 1",
              "Then": "bqtail.dummy_1"
            }
          ]
        }
      }
  }
  }
]
 ```


### Partition override

### Data transformation

### Data enrichement with side inputs


#### Destination

**Table**

Destination table uses the following syntax: [projectID]:datasetId.tableId

The following dynamic expression is supported:

- **$Mod(x)**, where x is a number, modulo is based datafile created unix time, i,e:  **mydataset.mytable_$Mod(4)**
- **$Date**, where data is based on datafile created time, i.e.: **mydataset.mytable_$Mod(40)_$Date**

**Pattern**

To derive table name from source path you can use pattern to define regular expression groups referenced by **$X** expression, X is the pattern occurence sequence.   

For example the following pattern: "data/(\\d{4})/(\\d{2})/(\\d{2})/.+", extracts 3 groups with $1, $2, and $3 respectively. 

With table defined as "proj:dataset:table_$1$2$3" and source URL "gs://bucket/data/2019/02/04/logs_xxx.avro" the specified table expands to: "proj:dataset:table_20190204"


"/nobid/adlog.request/(\\d{4})/(\\d{2})/(\\d{2})/.+"

### Deployment

See [Generic Deployment](../deployment/README.md) automation and post deployment testing