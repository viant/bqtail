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

The following google storage layout is used:

##### Data flow bucket

This bucket stores all configuration files:

**${configBucket}:**

```bash
    /
    | - BqTail
    |      |- config.json
    |      |- Rules
    |      |     | - rule1.json
    |      |     | - ruleN.json        
    | - BqDispatch    
    |      |- config.json        
        
```            

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


##### Export bucket

This bucket stores data exported from BigQuery, it can be source for [Storage Mirror FaaS](https://github.com/viant/smirror/) cloud function. 

**${exportBucket}**



# Deployment

### Install endly

[Download](https://github.com/viant/endly/releases/) latest binary

### Credentials setup
1. SSH credentials
https://github.com/viant/endly/tree/master/doc/secrets#ssh


2. Google Secrets for service account.

**TODO** add list of permission required to deploy CF


https://github.com/viant/endly/tree/master/doc/secrets#google-cloud-credentials
Copy google secret to ~/.secret/myProjectSecret.json 


## BqTail/BqDispatch

You can deploy the described infrastructure with BqTail and BqDispatch cloud function with [endly](https://github.com/viant/endly/) automation runner.

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment
endly run authWith=myProjectSecret region='us-central1'
```


### Testing deployments

All automation testing workflow copy rule to  gs://${configBucket}/BqTail/Rules/, 
followed by uploading data file to gs://${triggerBucket}/xxxxxx matching the rule, to trigger data ingestion.
In the final step the workflow waits and validate that data exists in dest tables.
TODO: remove cache file after adding a new rule


###### Synchronous CSV data ingestion test

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment/test/async
endly test authWith=myTestProjectSecrets.json
```
Where:
- [@rule.json](test/sync/rule.json)
- [@test.yaml](test/sync/test.yaml)


###### Asynchronous batched JSON data ingestion test

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment/test/async
endly test authWith=myTestProjectSecrets.json
```
Where:
- [@rule.json](test/async/rule.json)
- [@test.yaml](test/async/test.yaml)


###### Asynchronous partition override CSV data ingestion test

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment/test/override
endly test authWith=myTestProjectSecrets.json
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
endly deploy authWith=myGoogleSecret.json region=us-central1
```


#### Ingesting monitoring status

To ingest monitoring status:

1. Run the following DDL to create destination table 
    - [@schema.sql](../mon/schema/schema.sql)
2. Add the bqtail ingestion rule to gs://${opsConfig}/BqTail/Rules/sys/
    - [@bqmon.yaml](monitor/rule/bqmon.yaml)
    - [@bqjob.yaml](monitor/rule/bqjob.yaml)


