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

The goal of this project is to provide cost effective event driven data ingestion and extraction with generic Big Query events handler.
The first is implemented by [tail](tail/README.md) service, the latter by [dispatch](dispatch/README.md) service.

## Introduction


![BqTail](images/bqtail.png)

This project uses cloud functions to handle data ingestion and Big Query events.

- [Tail Service](tail/README.md)
- [Dispatch Service](dispatch/README.md)
- [Task Service](task/README.md)


## Usage


- Data ingestion in batches with 30 sec time window.

#bqtail.config
```json
{
  "BatchURL": "gs://my-ops-bucket/batch/",
  "ErrorURL": "gs://my-ops-bucket/errors/",
  "JournalURL": "gs://my-ops-bucket/journal/",
  "DeferTaskURL": "gs://my-ops-bucket/tasks/",
  "Routes": [
    {
      "When": {
        "Prefix": "/data/case001",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "bqtail.dummy"
      }
    }
  ]
 }
```
 


## Deployment


## End to end testing

## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

<a name="Credits-and-Acknowledgements"></a>

## Credits and Acknowledgements

**Library Author:** Adrian Witas

