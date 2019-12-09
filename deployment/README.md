# Deployment

The following document describes generic shared deployments for various rules, with one
BqTail and BqDispatch cloud functions per project.


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

## BqTail/BqDispatch

You can deploy the described infrastructure with BqTail and BqDispatch cloud function with [endly](https://github.com/viant/endly/) automation runner.

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment
endly run authWith=myGoogleSecret.json
```


### Testing deployments

All automation testing workflow copy rule to  gs://${configBucket}/BqTail/Rules/, 
followed by uploading data file to gs://${triggerBucket}/xxxxxx matching the rule, to trigger data ingestion.
In the final step the workflow waits and validate that data exists in dest tables.


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

You can deploy the described infrastructure Monitor cloud function with [endly](https://github.com/viant/endly/) automation runner.

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment/monitor
endly deploy authWith=myGoogleSecret.json
```



## Monitoring 


[BqTailMonitor](../mon) can be used to monitor bq tail performance for each table destination.


**On Google Cloud Platform:**

```bash
curl --data '{"IncludeDone":false}' -X POST  -H "Content-Type: application/json"  $monitorEndpoint
```
