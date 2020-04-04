# Deployment

The following document describes generic shared deployments for various rules, with one
BqTail and BqDispatch cloud functions per project.


It is highly recommended to use transient project just for bqtail ingestion with transient dataset option. 
In this case you do not count load/query/copy jobs towards project level quota, and still have ability
to ingest data to your final project's dataset.
If there is no transformation option used, after loading data to transient table, data is appended to dest project table with 
If no transformation option is used data is appended with copy operation which is free of charge, otherwise
regular SQL usage pricing applies.


### Google Storage layout:

The following google storage layout is used to deploy and operate serverless data ingestion with  BqTail:

##### Configuration bucket

This bucket stores all configuration files:

**${configBucket}:**

```bash
    /
    | - BqTail
    |      |- config.json
    |      |- Rules
    |      |     | - rule1.yaml
    |      |     | - group_folder 
    |      |     |      - rule2.yaml        
    |      |     | - ruleN.json        
    | - BqDispatch    
    |      |- config.json        
        
```            

Configuration bucket stores
  - [BqTail configuration](https://github.com/viant/bqtail/tree/master/tail#configuration) 
  - [BqDispatch configuation](https://github.com/viant/bqtail/tree/master/dispatch#configuration)
  - [Data ingestion rule](https://github.com/viant/bqtail/tree/master/tail#data-ingestion-rules)

Once data arrives to trigger bucket, BqTail matches datafile with a rule to start ingestion process.

##### Operations bucket

This bucket stores all transient data and history journal including errors 

**${opsBucket}:**

```bash
    /
    | - BqTail/errors
    | - BqDispatch/errors
```



##### Trigger Bucket

This bucket stores all data that needs to be ingested to Big Query, 

**${triggerBucket}**


```bash
    /
    | - processX/YYYY/MM/DD/tableL_0000xxx.avro
    | - processZ/YYYY/MM/DD/tableM_0000xxx.avro

```

##### Dispatcher bucket

This bucket is used by BqDispatcher to manges scheduled async batches and BigQuery running jobs.

##### Export bucket

This bucket stores data exported from BigQuery, it can be source for [Storage Mirror FaaS](https://github.com/viant/smirror/) cloud function. 

**${exportBucket}**


# Deployment

### Install endly

[Download](https://github.com/viant/endly/releases/) latest binary

### Credentials setup
1. SSH credentials
    - [Create SSH credentials](https://github.com/viant/endly/tree/master/doc/secrets#ssh)
    
    On OSX make sure that you have SSH remote login enabled
    ```bash
    sudo systemsetup -setremotelogin on
    ```
2. Google Secrets for service account.
    - [Create service account secrets](https://github.com/viant/endly/tree/master/doc/secrets#google-cloud-credentials)
    - Set role required by cloud function/scheduler deployment
         - Cloud Function admin 
         - Editor
    - Copy google secret to ~/.secret/myProjectSecret.json 
3. Slack credentials (optionally)

The slack credentials uses the following JSON format

@slack.json
```json
{
  "Token": "MY_VALID_OAUTH_SLACK_TOKEN"
}
```
To encrypt slack in google storage with KMS you can run the following command
```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment
endly secure_slack authWith=myProjectSecret slackOAuth=slack.json
```


## BqTail/BqDispatch

You can deploy the described infrastructure with BqTail and BqDispatch cloud function with [endly](https://github.com/viant/endly/) automation runner.

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment
endly run authWith=myProjectSecret region='us-central1'
```

#### Deployment checklist

Once deployment is successful you can check
1. The following buckets are present
     - **${PROJECT_ID}_config** (configuration bucket)
     - **${PROJECT_ID}_operation** (journal bucket)
     - **${PROJECT_ID}_bqtail** (cloud functiontrigger bucket)
     - **${PROJECT_ID}_bqdispatch** (bqdispatch bucket)
2. The following cloud functions are present (check logs for error)
     - **BqTail**
     - **BqDispatch**
3. The following Cloud Scheduler is present (check for successful run)
      - **BqDispatch** with successful run
   
### Testing deployments

All automation testing workflow copy rule to  gs://${configBucket}/BqTail/Rules/, 
followed by uploading data file to gs://${triggerBucket}/xxxxxx matching the rule, to trigger data ingestion.
In the final step the workflow waits and validate that data exists in dest tables.

When you test a new rule manually, upload the rule to gs://${configBucket}/BqTail/Rules/.

**Make sure to remov**e _gs://${configBucket}/BqTail/_.cache_ file if it is present before uploading datafile to trigger bucket. 



###### Asynchronous batched JSON data ingestion test

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment/test/async
endly test authWith=myTestProjectSecrets
```
Where:
- [@rule.json](test/async/rule.json)
- [@test.yaml](test/async/test.yaml)


**Post run check**

In the Cloud function Log you should be able to see the following:

- Successful batching events (BqTail log stream) for each file (2 files):

```json
{"Batched":true,"EventID":"1086565206770154","IsDataFile":true,,"Matched":true,"MatchedURL":"gs://xx_bqtail/deployment_test/async/2020-04-04T11:43:30-07:00/dummy_1.json","Retriable":true,"RuleCount":34,"Started":"2020-04-04T18:43:31Z","Status":"ok","TimeTakenMs":5291,"TriggerURL":"gs://xx_bqtail/deployment_test/async/2020-04-04T11:43:30-07:00/dummy_1.json","Window":{"Async":true,"DestTable":"xx:test.dummy","DoneProcessURL":"gs://xx_operation/BqTail/Journal/Done/xx:test.dummy/2020-04-04_18/1086565206770154.run","End":"2020-04-04T18:44:00Z","EventID":"1086565206770154","FailedURL":"gs://xx_operation/BqTail/Journal/failed","ProcessURL":"gs://xx_operation/BqTail/Journal/Running/xx:test.dummy--1086565206770154.run","RuleURL":"gs://xx_config/BqTail/Rules/deployment_async_test.json","Source":{"Status":"pending","Time":"2020-04-04T18:43:30Z","URL":"gs://xx_bqtail/deployment_test/async/2020-04-04T11:43:30-07:00/dummy_1.json"},"Start":"2020-04-04T18:43:30Z","URL":"gs://xx_bqdispatch/BqDispatch/Tasks/xx:test.dummy_1179878484004789046_1586025840.win"}}
```

```json
{"Batched":true,"BatchingEventID":"1086562538339341","EventID":"1086562538339341","IsDataFile":true,"ListOpCount":34,"Matched":true,"MatchedURL":"gs://xx_bqtail/deployment_test/async/2020-04-04T11:43:30-07:00/dummy_2.json","Retriable":true,"RuleCount":34,"Started":"2020-04-04T18:43:40Z","Status":"ok","TimeTakenMs":269,"TriggerURL":"gs://xx_bqtail/deployment_test/async/2020-04-04T11:43:30-07:00/dummy_2.json","WindowURL":"gs://xx_bqdispatch/BqDispatch/Tasks/xx:test.dummy_1179878484004789046_1586025840.win"} BqTail 1086562538339341 
```
- Successful batch scheduling (BqDispatch log stream)
- Load job submission with batch runner (BqTail log stream)
- BigQuery Load job completion notification (BqDispatch log stream) 
- Big Query copy job submission from transient table to dest table (BqTail  log stream)
- BigQuery Copy job completion notification (BqDispatch log stream)
- Data should be present in destination table.


BqDispatch Log example:
```json
{
  "BatchCount": 1,
  "Batched": {
    "gs://xx_bqdispatch/BqDispatch/Tasks/xx:test.dummy_1179878484004789046_1586025840.win": "2020-04-04T22:13:00Z"
  },
  "Cycles": 17,
  "Jobs": {
    "Jobs": {
      "gs://xx_bqdispatch/BqDispatch/Tasks/proj:xx:US/xx:test.dummy-1179878484004789046_00001_load--dispatch": {
        "Project": "",
        "Region": "",
        "ID": "xx_test_dummy--1179878484004789046_00001_load--dispatch",
        "URL": "gs://xx_bqdispatch/BqDispatch/Tasks/proj:xx:US/xx:test.dummy--1179878484004789046_00001_load--dispatch",
        "Status": "DONE"
      },
      "gs://xx_bqdispatch/BqDispatch/Tasks/proj:xx:US/xx:test.dummy--1179878484004789046_00002_copy--dispatch": {
        "Project": "",
        "Region": "",
        "ID": "xx_test_dummy--1179878484004789046_00002_copy--dispatch",
        "URL": "gs://xx_bqdispatch/BqDispatch/Tasks/proj:xx:US/xx:test.dummy--1179878484004789046_00002_copy--dispatch",
        "Status": "DONE"
      }
    }
  },
  "Performance": {
    "xx": {
      "ProjectID": "xx",
      "Running": {
        "LoadJobs": 1,
        "BatchJobs": 1
      },
      "Pending": {},
      "Dispatched": {
        "CopyJobs": 1,
        "LoadJobs": 1
      },
      "Throttled": {}
    }
  },
  "Started": "2020-04-04T22:13:04Z",
  "Status": "ok",
  "TimeTakenMs": 55000
}
```

**Note that**, When datafile is not matched with ingestion rule it returns "Status":"noMatch" 



###### Asynchronous partition override CSV data ingestion test

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment/test/override
endly test authWith=myTestProjectSecrets
```

Where:
- [@rule.json](test/override/rule.json)
- [@test.yaml](test/override/test.yaml)



##### More rules examples

You can find more example for various configuration setting in [end to end tetst cases](https://github.com/viant/bqtail/tree/master/e2e)


## Monitoring

Deploy monitor with scheduler

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment/monitor
endly deploy authWith=myProjectSecret region=us-central1
```


#### Ingesting monitoring status

To ingest monitoring status:

1. Run the following DDL to create destination table 
    - [@schema.sql](../mon/schema/schema.sql)
2. Add the bqtail ingestion rule to gs://${opsConfig}/BqTail/Rules/sys/
    - [@bqmon.yaml](monitor/rule/bqmon.yaml)
    - [@bqjob.yaml](monitor/rule/bqjob.yaml)


