# Deployment

The following document describes global shared data flow BqTail deployments for various data flow processes, with one
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
    |      |     | - process1_routes.json
    |      |     | - processN_routes.json        
    | - BqDispatch    
    |      |- config.json        
    |      |- rules
    |      |     | - process1_routes.json
    |      |     | - processN_routes.json        
        
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

This bucket stores data exported from BigQuery, it can be source for [storage mirror FaaS](https://github.com/viant/smirror/) cloud function. 

**${exportBucket}**



# Deployment

You can deploy the described infrasturctre with BqTail and BqDispatch cloud function with [endly](https://github.com/viant/endly/) automation runner.

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment
endly run authWith=myGoogleSecret.json
```
