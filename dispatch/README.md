# Dispatch Service

Dispatch service allows you run cloud service task for defer BqTail task or specified source or Big Query destination or job type.
  
- [Introduction](#introduction)
- [Usage](#usage)
- [Deployment](#deployment)

## Introduction 

![BqDispatch](../images/dispatch.png)  


Dispatch service run specified task by matching BigQuery Job.
The first matching strategy uses simple JobID matching with google storage ${config.DeferTaskURL}/${JobID}.json, where
jobID has to have suffix: 'dispatch'. The dispatch.json file defines [defer actions.go](../task/actions.go),
for example: [@$JobID.json](usage/dispatch.json) defines on success and or failure actions.

```json
{
  "DeferTaskURL": "gs://${config.Bucket}/tasks/",
  "Async": true,
  "JobID": "~/\\d+/",
  "OnSuccess": [
    {
      "Action": "delete",
      "Request": {
        "SourceURL": "gs://${config.Bucket}/data/dataset/table.json"
      }
    }
  ],
  "OnFailure": [
    {
      "Action": "move",
      "Request": {
        "SourceURL": "gs://${config.Bucket}/data/dataset/table.json",
        "DestURL": "gs://${config.Bucket}/errors"
      }
    }
  ]
}
``` 



Another matching method uses configuration routes with [When filter](config/filter.go), 
for example: [@config.json](usage/config.json) defines routes and on success and or failure actions.

```json
{
  "DeferTaskURL": "gs://${config.Bucket}/tasks/",
  "Routes": [
    {
      "When": {
        "Dest": ".+:mydataset\\.my_table_v2",
        "Type": "QUERY"
      },
      "OnSuccess": [
        {
          "Action": "export",
          "Request": {
            "DestURL": "gs://${config.Bucket}/export/my_table.json.gz"
          }
        }
      ]
    },
    {
      "When": {
        "Dest": ".+:mydataset\\.my_table_v3",
        "Type": "LOAD"
      },
      "OnSuccess": [
        {
          "Action": "copy",
          "Request": {
            "Dest": "mydataset.my_table_v4"
          }
        }
      ]
    }
  ]
}
``` 


## Usage

### Configuration

**Configuration options:**

- JournalURL: job history location 
- ErrorURL: - errors location
- DeferTaskURL: transient storage location for managing deferred tasks (both BqTail and BqDispatch have to use the same URL) 
- Routes: post job tasks matching rules (only one route can be matched)





### Deployment

```bash
gcloud functions deploy BqDispatch --entry-point BqDispatchFn --trigger-resource projects/MY_PROJECT_ID/jobs/{jobId} --trigger-event google.cloud.bigquery.job.complete  \n
 --set-env-vars=CONFIG=gs://YYY/config/bqdispatch.json
--runtime go111
```

Where:
- XXX is data bucket name
- bqdispatch.json is configuration file
```json
{
  "ErrorURL": "gs://YYY/errors/",
  "JournalURL": "gs://YYY/journal/",
  "DeferTaskURL": "gs://YYY/tasks/",
  "Routes": [
    {
      "When": {
        "Dest": ".+:mydataset\\.mytable",
        "Type": "QUERY"
      },
      "OnSuccess": [
        {
          "Action": "export",
          "Request": {
            "DestURL": "gs://${config.Bucket}/export/dummy.json.gz"
          }
        }
      ]
    },
    {
      "When": {
        "Dest": ".+:mydataset\\.mytable2",
        "Type": "LOAD"
      },
      "OnSuccess": [
        {
          "Action": "copy",
          "Request": {
            "Dest": "mydataset.mytable3"
          }
        }
      ]
    }
  ]
}

```
