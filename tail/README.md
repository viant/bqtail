## Tail Service

Tail service ingests data to Big Query with Load API. 

- [Introduction](#introduction)
- [Usage](#usage)
- [Deployment](#deployment)

### Introduction 

![BqTail](../images/bqtail.png)  


### Usage

#### Batch ingestion

By default individual 


#### Synchronous mode


#### Asynchronous mode


#### Transient Dataset


#### Data deduplication


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
gcloud functions deploy XXXBQTailFn --entry-point BqTail --trigger-resource XXX --trigger-event google.storage.object.finalize  \n
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
"DispatchURL": "gs://YYY/events/",
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

