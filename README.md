# BigQuery Tail (bqtail)

This library is compatible with Go 1.11+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Motivation](#motivation)
- [Introduction](#introduction)
- [Tail Service](tail/README.md)
- [Dispatch Service](dispatch/README.md)
- [Usage](#usage)
- [End to end testing](#end-to-end-testing)



## Motivation

The goal of this project is to provide robust and cost effective events driven, data ingestion to Big Query.
BqTail elegantly addresses all Big Query limitations/restriction/quotas (load jobs per table, project, corrupted data files) with batching and transient dataset approach.
In addition it also provides data transformation enrichment and deduplication capabilities.

This project is used by Viant to ingest **70+ billions** transactions _daily_, **1.4 million files** to 100+ tables, all under $15, as cost effective alternative
for Big Query Streaming API, Cloud Dataflow and [in house build cost optimized ETL](https://github.com/viant/etly) framework. 


## Introduction

![BqTail](images/bqtail.png)


- [Tail Service](tail/README.md)
- [Dispatch Service](dispatch/README.md)
- [Task Service](service/README.md)


## Usage

##### **Data ingestion**

The following define rule to ingest data in batches within 30 sec time window in async mode.

[@rule.json](usage/batch/rule.json)
```json
[
    {
      "When": {
        "Prefix": "/data/",
        "Suffix": ".avro"
      },
      "Dest": {
        "Table": "mydataset.mytable"
      },
      "Async": true,
      "Batch": {
        "Window": {
          "DurationInSec": 30
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


##### **Data ingestion with deduplication**

The following define rule to ingest data in batches within 60 sec time window in async mode.

[@rule.json](usage/dedupe/rule.json)
```json
 [
    {
      "Async": true,
      "When": {
        "Prefix": "/data/",
        "Suffix": ".avro"
      },
      "Dest": {
        "Table": "mydataset.mytable",
        "TempDataset": "transfer",
        "UniqueColumns": ["id"]
      },
      "Batch": {
        "Window": {
          "DurationInSec": 60
        }
      },
      "OnSuccess": [
        {
          "Action": "query",
          "Request": {
            "SQL": "SELECT '$JobID' AS job_id, COUNT(1) AS row_count, CURRENT_TIMESTAMP() AS completed FROM $DestTable",
            "Dest": "mydataset.summary"
          }
        },
        {
          "Action": "delete"
        }
      ],
      "OnFailure": [
        {
          "Action": "notify",
          "Request": {
            "Channels": [
              "#e2e"
            ],
            "From": "BqTail",
            "Title": "bqtail.wrong_dummy ingestion",
            "Message": "$Error",
            "Token": "SlackToken"
          }
        }
      ]
    }
]
```

##### **Data ingestion with partition override**

[@rule.json](usage/override/rule.json)
```json
[
  {
    "When": {
      "Prefix": "/data/",
      "Suffix": ".csv"
    },
    "Async": true,
    "Dest": {
      "Override": true,
      "Table": "myproject:mydataset.mytable",
      "Partition": "$Date",
      "TransientDataset": "temp",
      "SkipLeadingRows": 1,
      "MaxBadRecords": 3,
      "FieldDelimiter": ",",
      "IgnoreUnknownValues": true
    },
    "OnSuccess": [
      {
        "Action": "delete"
      }
    ]
  }
]
```


## Cost optimized serverless

The following snapshot show serverless cost overhead per one day data ingestion (70TB, 1.6 millions files).

![BqTail](images/serverless_cost.png)

Note that actual data ingestion with load and copy BigQuery operations are free of charge.


## Deployment

The following [link](deployment/README.md) details generic deployment.


## End to end testing

Bqtail is fully end to end test with including batch allocation stress testing with 2k files.

You can try on all data ingestion by simply running e2e test cases:

- [Prerequisites](e2e/README.md#prerequisites)
- [Use cases](e2e/README.md#use-cases)




## Contributing to bqtail

BqTail is an open source project and contributors are welcome!

See [TODO](TODO.md) list

## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

<a name="Credits-and-Acknowledgements"></a>

## Credits and Acknowledgements

**Library Author:** Adrian Witas

