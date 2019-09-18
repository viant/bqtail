# Deployment

The following document describes global shared data flow BqTail deployments for various data flow processes, with one
BqTail and BqDispatch cloud functions per project.


### Google Storage layout:

The following google storage layout is used:

##### Data flow bucket

This bucket stores all configuration files:

**${gcp.projectID}-dataflow:**


```bash
    /
    | - config
    |      |- dispatch.json
    |      |- dispatch
    |      |     | - process1_routes.json
    |      |     | - processN_routes.json        
    |      |- tail.json        
    |      |- tail
    |      |     | - process1_routes.json
    |      |     | - processN_routes.json        
        
```            

##### Operational data flow bucket

This bucket stores all transient cloud function data and errors and history journal 

**${gcp.projectID}-dataflow-ops**

##### Inbound data flow bucket

This bucket stores all data that needs to be ingested to Big Query, 

**${gcp.projectID}-dataflow-indound**


##### Outbound data flow bucket

This bucket stores data exported from BigQuery, it can be source for [storage mirror FaaS](https://github.com/viant/smirror/) cloud function. 

**${gcp.projectID}-dataflow-outbound**


# Deployment

With [endly](https://github.com/viant/endly/) automation runner

```bash
git checkout https://github.com/viant/bqtail.git
cd bqtail/deployment
endly run
```
