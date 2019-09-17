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

The following example defines single route to ingested any JSON file matching /data/folder prefix to mydataset.mytable.

In this configuration scenario each data file is ingested  to Big Query by dedicated cloud function instance.
After initiating a load Job, the function waits for job completion to finally run on success tasks. 

This per data file synchronous mode is very cost inefficient, since each data file uses dedicated cloud function for the whole data ingestion duration.
Another limitation that needs to be consider is number of data files, if you expect more than 1K per day you reach daily load job count restriction.
On top of that if Big Query load job takes more than cloud function max execution time, the function is terminated 
and the post task execution does not run at all.

All these limitations are addressed by asynchronous and batch mode. 
 
[@config/bqtail.json](usage/sync.json)
```json
{
  "ErrorURL": "gs://YYY/errors/",
  "JournalURL": "gs://YYY/journal/",
  "Routes": [
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
}
```

**Configuration options:**

- JournalURL: job history location 
- ErrorURL: - errors location
- DeferTaskURL: transient storage location for managing deferred tasks (both BqTail and BqDispatch have to use the same URL) 
- BatchURL: transient storage location for managing event batching.
- Routes: data ingestion matching rules (only one route can be matched)


#### Asynchronous mode

In asynchronous mode BqTailFn cloud function schedules post task execution using deferred task base URL and submit load job. 
Once BigQuery job completes it is picked up by BqDispatchFn cloud function to run post tasks. 

In this mode cloud function execution time is stremlined to actual task run without unnecessary waits.


[@config/bqtail.json](usage/async.json)
```json
{
  "ErrorURL": "gs://YYY/errors/",
  "JournalURL": "gs://YYY/journal/",
  "DeferTaskURL": "gs://e2e-data/tasks/",
  "Routes": [
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
}
```

#### Batch ingestion

In batch mode, storage event are batch by time window, in this mode only one cloud function manages batching while others ends
after scheduling event source.  The batching process is details [here](batch/README.md).

Batch URL is used to manged batch windowing process.

The following configuration specify batch sync mode.

[@config/bqtail.json](usage/batch.json)
```json
{
  "ErrorURL": "gs://YYY/errors/",
  "BatchURL": "gs://e2e-data/batch/",
  "JournalURL": "gs://YYY/journal/",
  "Routes": [
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
}
```

The following configuration specify batch asynchronous  mode.

[@config/bqtail.json](usage/async_batch.json)
```json
{
  "ErrorURL": "gs://YYY/errors/",
  "BatchURL": "gs://e2e-data/batch/",
  "JournalURL": "gs://YYY/journal/",
  "DeferTaskURL": "gs://e2e-data/tasks/",
  "Routes": [
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
}
```


#### Transient Dataset

When ingesting data, from one or many datafiles, some entries may be corrupted impacting data quality.
To add extra data quality check you use  transient dataset. In this case data is moved to destination table
only if temp table data ingestion was successful.
Temp table is constructed from destination table suffixed by event ID.

The following configuration specify transient dataset.


[@config/bqtail.json](usage/transient.json)
```json
{
  "ErrorURL": "gs://YYY/errors/",
  "JournalURL": "gs://YYY/journal/",
  "Routes": [

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
}

```

#### Data deduplication

When using transient table you can specify unique columns to deduplicate data while moving to destination table.


[@config/bqtail.json](usage/dedupe.json)
```json
{
  "ErrorURL": "gs://YYY/errors/",
  "JournalURL": "gs://YYY/journal/",
  "BatchURL": "gs://e2e-data/batch/",
  "DeferTaskURL": "gs://e2e-data/tasks/",
  "Routes": [
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
}
```

#### Template table



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



### Deployment

```bash
gcloud functions deploy XXXBQTail --entry-point BqTailFn --trigger-resource XXX --trigger-event google.storage.object.finalize  \n
 --set-env-vars=CONFIG=gs://YYY/config/bqtail.json
--runtime go111
```

Where:
- XXX is data bucket name
- YYY is confiuration/meta/logging bucket name
- bqtail.json is configuration file
```json
{

"BatchURL": "gs://YYY/batch/",
"ErrorURL": "gs://YYY/errors/",
"JournalURL": "gs://YYY/journal/",
"DeferTaskURL": "gs://YYY/tasks/",
"Routes": [
  {
    "When": {
      "Prefix": "/data/",
      "Suffix": ".json"
    },
    "Dest": {
      "Table": "mydataset.mytable"
    }
  }]
}

```

