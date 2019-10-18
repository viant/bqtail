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

The goal of this project is to provide cost effective events driven, data ingestion and extraction with Big Query.

## Introduction


![BqTail](images/bqtail.png)


- [Tail Service](tail/README.md)
- [Dispatch Service](dispatch/README.md)
- [Task Service](task/README.md)


## Usage


- **Data ingestion**

The following define rule to ingest data in batches within 30 sec time window in async mode.

[@myrule.json](usage/batch/rule.json)
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
      ],
      "OnFailure": [
        {
          "Action": "move",
          "Request": {
            "DestURL": "gs://myBucket/errors"
          }
        }
      ]
    }
]
```


- **Data ingestion with deduplication**

The following define rule to ingest data in batches within 60 sec time window in async mode.

[@config/bqtail.json](usage/dedupe/rule.json)
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
          "Action": "move",
          "Request": {
            "DestURL": "gs://myBucket/errors"
          }
        },
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

- **Data extraction**

The following define rule to extract data to google storate after target table is modified.

[@config/bqtail.json](usage/ingest/rule.json)
```json
[
 {
   "When": {
     "Dest": ".+:mydataset\\.mytable",
     "Type": "QUERY"
   },
   "OnSuccess": [
     {
       "Action": "export",
       "Request": {
         "DestURL": "gs://${config.Bucket}/export/mytable.json.gz"
       }
     }
   ]
 }
]
```

## Deployment

**Prerequisites**

The following URL are used by tail/dispatch services:

- JournalURL - job history journal 
- ErrorURL - job that resulted in an error
- DeferTaskURL - transient storage for managing deferred tasks (tail in async mode). 
- BatchURL - transient storage for managing event batching.

**Cloud function deployments**

- [BqTail](tail/README.md#deployment)
- [BqDispatch](dispatch/README.md#deployment)


The following [Deployment](deployment/README.md) details generic deployment.



### Monitoring


- Check for any files under ErrorURL
- DeferTaskURL should not have very old files, unless there is processsing error
- BatchURL should not have very old files, unless there is processing error



## End to end testing

You can try on all data ingestion and extraction scenarios by simply running e2e test cases:

- [Prerequisites](e2e/README.md#prerequisites)
- [Use cases](e2e/README.md#use-cases)

## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

<a name="Credits-and-Acknowledgements"></a>

## Credits and Acknowledgements

**Library Author:** Adrian Witas

