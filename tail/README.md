# Tail Service

Tail service ingests data to Big Query with Load/Copy or Query API. 

- [Introduction](#introduction)
- [Usage](#usage)
- [Deployment](#deployment)

## Introduction 

Tail service is a google storage triggered cloud function that: 


- Matches incoming data files with specified ingestion rules
- Batches matched incoming data files 
- Submit BigQuery Load Job for matched async batch tasks triggered by the dispatch service
- Run post actions (on success or failure) for matched post BigQuery Job actions triggered by the dispatch service 
- [Post actions](../service/README.md) 
    - BigQuery (copy/query/export)
    - Storage (move/delete)
    - Slack (notify)
    - Pubsub (publish)
    - HTTP API (call)


## Configuration

Configuration is defined as [config.go](config.go)

**Configuration options:**

- JournalURL: active/past job journal URL 
- ErrorURL: - errors location
- AsyncTaskURL: transient storage location for managing async batches and BigQuery job post actions 
- SyncTaskURL: transient storage location for managing batch load job in sync mode.
- RulesURL: base URL where each rule is JSON or YAML file with one or more rule
- CorruptedFileURL: url for corrupted files
- InvalidSchemaURLL: url for incompatible schema files
- TriggerBucket - trigger bucket
- ActiveLoadJobURL: currently running data ingestion jobs URL
- DoneLoadJobURL: past data ingestion jobs URL
- SlackCredentials


**Note:**
To reduce Storage Class A operations cost: cache file is used for config files:  delete cache file alongside adding a new rule.


### Data ingestion rules

Individual rules are defined in JSON or YAML format. The following is example of asynchronous batched data ingestion:


[@rule.yaml](usage/async.yaml) 
```yaml
When:
  Prefix: "/data/folder"
  Suffix: ".json"
Async: true
Batch:
  Window:
    DurationInSec: 90
Dest:
  Table: mydataset.mytable
OnSuccess:
  - Action: delete
OnFailure:
  - Action: move
    Request:
      DestURL: gs://e2e-data/errors
```

or

[@rule.json](usage/async.json)
```json
{
  "When": {
    "Prefix": "/data/folder",
    "Suffix": ".json"
  },
  "Async": true,
   "Batch": {
      "Window": {
          "DurationInSec": 90
      }
   },
  "Dest": {
    "Table": "mydataset.mytable"
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
        "DestURL": "gs://e2e-data/errors"
      }
    }
  ]
}
```


**Individual rule** can has the following attributes:

- Async: asynchronous mode flag, always set to true to use async mode which is cost effect and robust.
- [Dest](#data-destination): data destination with transformation rules
- When defines matching filter
  - Prefix: path prefix or
  - Suffix: path suffix or
  - Filter: path regexp
 
- MaxReload: maximum load attemps, where each attempt excludes reported corrupted locations (15 default)  
- Batch: specified batch window, when specifying window make sure that number of batches never exceed 1K per day.
- OnSuccess: actions to run when job completed without errors
- OnFailure: actions to run when job completed with errors
 
Post actions can use predefined [Cloud Service](../service/README.md) operation.


#### Data destination  

Dest supports the following attributes:

Besides you can also specify any attribute from [bigquery.JobConfigurationLoad](https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad) load job configuration contract.
For example:
```yaml
Dest:
   Table: myproject:mydataset.myTable
   AllowJaggedRows: true
   AllowQuotedNewlines: true
```



- **Table**

    Destination table uses the following syntax: [projectID]:datasetId.tableId
    
    The following dynamic expression is supported:

    - **$Mod(x)**, where x is a number, modulo is based datafile created unix time, i,e:  **mydataset.mytable_$Mod(4)**
    - **$Date**, where data is based on datafile created time, i.e.: **mydataset.mytable_$Mod(40)_$Date**

- **Pattern**

    To derive table name from source path you can use pattern to define regular expression groups referenced by **$X** expression, X is the pattern occurrence sequence.   

    You can use the following [RegExpr](https://github.com/google/re2/wiki/Syntax) syntax.
    
    For example the following pattern: "data/(\\d{4})/(\\d{2})/(\\d{2})/.+", extracts 3 groups with $1, $2, and $3 respectively. 
    
    With table defined as "proj:dataset:table_$1$2$3" and source URL "gs://bucket/data/2019/02/04/logs_xxx.avro" the specified table expands to: "proj:dataset:table_20190204"
    
    
    "/nobid/adlog.request/(\\d{4})/(\\d{2})/(\\d{2})/.+"


- **Parameters** name pattern substitution parameters 
 
The following rule shows example of 3 parameters scraped from URL     
```yaml
When:
  Prefix: "/data/case${parentIndex}/"
  Suffix: ".json"
Dest:
  Table: bqtail.dummy_v${parentIndex}_$MyTableSufix
  Pattern: /data/case(\d+)/(\d{4})/(\d{2})/(\d{2})/
  Parameters:
    - Name: MyTableSufix
      Expression: $2$3$4
    - Name: MyDate
      Expression: $2-$3-$4
    - Name: CaseNo
      Expression: '$1'

  Transient:
    Dataset: temp

  Transform:
    date: DATE('$MyDate')
    use_case: "'$CaseNo'"

OnSuccess:
  - Action: delete
```    


- **AllowFieldAddition**: flag to enable automatic failed addition, 
    - For JSON source format, bqtail detect and patched template and dest table 
    - For AVRO/PARQUET format: bqtail set the following Load job options: 
        - Dest.SchemaUpdateOptions: ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]
  **Override** dest table override flag (append by default)
- **Partition** dest table partition.
- **Schema** defines dest table schema
  * **Template**: destination table template, when specified destination table will be created if it does not exists
  * **Autodetect**: flag to autodetect schema during load 
  * **Split**: dynamic destination split rules based on data content


- **Expiry**:  optional destination table expiry expression like: 1min, 2hours, 3months, 1 year etc ...
Note that this option would **expire/remove a table** once expiry is passed,  expiry is calculated based on ingestion process start.
For example if your batch window is 2 min, and expiry is less, load job may fail, since dest table would expiry before batch start.

For example:
```yaml
Dest:
   Table: myproject:mydataset.myTable
   Expiry: 1hour
```

## Transient data flow

When transient option is used data is load to transient table in transient dataset and project.
The transient table name is formed from destination table suffixed by event ID, which makes is always unique. 
Note that load jobs NEVER count against destination table quota.
In addition with project **Balancer** option bqtail is never not a subject to 100K jobs per day quota.

Once if data is successfully loaded, it is copied to destination table (in append mode by default).
At this point it is possible to apply various data transformation with UniqueColumns, Transform expressions or Split options.
If rule does not specify any transformation option bqtail use BigQuery CopyJob to transfer data from temp table to destination, 
otherwise QueryJob with destination table is used. You can control this behavior with CopyMethod option.


- **Transient** transient settings (for dedicated ingesting project settings)
   * **Dataset** transient dataset. (It is recommended to always used transient dataset)
   * **ProjectID** transient project
   * **Balancer** multi projects balancer settings
   * **Template** transient table template
   * **Criteria** optional criteria added where coping data from temp to dest without Split option
   * **CopyMethod** control transient to dest table data copy with one of the following
        - COPY (BigQueryCopyJob), 
        - QUERY (BigQueryQueryJob with SELECT FROM and destination table)
        - DML(BigQueryQueryJob with INSERT AS SELECT DML)

        When transformation options is used or transient template has extra column that not exists in destination 
        you can only used Query or DML CopyMethod (Query is default).


- **UniqueColumns** deduplication unique columns
- **Transform** map of dest table column with transformation expression
- **SideInputs** transformation left join tables.


#### Partition override

For daily data ingestion you can use the following rule to override individual partition at a time.


[@rule.json](usage/override.json)
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
      "Transient": {"Dataset": "temp"},
      "SkipLeadingRows": 1,
      "MaxBadRecords": 3,
      "FieldDelimiter": ",",
      "IgnoreUnknownValues": true
    },
    "OnSuccess": [
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
          "Title": "Failed to load $Source to ${gcp.ProjectID}:test.dummy",
          "Message": "$Error"
        }
      }
    ]
  }
]

```

#### Template table

In case destination table does not exists you can specify schema source table with schema.template attribute.

[@config/template.json](usage/template.json)
```json
[
    {
      "When": {
        "Prefix": "/data/folder",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "mydataset.mytable",
        "Schema": {
          "Template": "mydataset.template_table"
        }
      }
   }
]
```


#### Transient Dataset

When ingesting data, from one or many datafiles, some entries may be corrupted impacting data quality.
To add extra data quality check you can use transient dataset. In this case data is moved to destination table
only if temp table data ingestion was successful.

Additional benefits of transient transfer is using dedicated transient project for ingestion only, where BqDispatch only
get ingestion notification. Finally separating transinet and final destination project allows you to better control 
various BigQuery limits lik 1K load jobs per table or 100K load jobs per project.


Temp table is constructed from destination table suffixed by event ID.

The following configuration specify transient dataset.
[@config/transient.json](usage/transient.json)
```json

    {
      "When": {
        "Prefix": "/data/folder",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "mydataset.mytable",
        "Transient": {"Dataset": "temp"}
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ]
    }
```



#### Data deduplication

When using transient table you can specify unique columns to deduplicate data while moving to destination table.


[@config/dedupe.json](usage/dedupe.json)
```json

  {
      "Async": true,
      "When": {
        "Prefix": "/data/folder",
        "Suffix": ".json"
      },
      "Dest": {
        "Table": "mydataset.mytable",
        "Transient": {"Dataset": "temp"},
        "UniqueColumns": [
          "id"
        ]
      },
      "Batch": {
        "Window": {
          "DurationInSec": 80
        }
      },
      "OnSuccess": [
        {
          "Action": "delete"
        }
      ]
  }
```



#### Dynamic table destination based on source data.


To dynamically rule data based on source data values you can use the following rule.

[@config/dynamic_dest.json](usage/dynamic_dest.json)

```json
  {
    "When": {
      "Prefix": "/data/case013",
      "Suffix": ".json"
    },
    "Async": true,
    "Dest": {
      "Table": "bqtail.dummy",
      "Transient": {"Dataset": "temp"},
      "Schema": {
        "Template": "bqtail.dummy",
        "Split": {
          "ClusterColumns": [
            "id",
            "info.key"
          ],
          "Mapping": [
            {
              "When": "MOD(id, 2) = 0",
              "Then": "bqtail.dummy_0"
            },
            {
              "When": "MOD(id, 2) = 1",
              "Then": "bqtail.dummy_1"
            }
          ]
        }
      }
  }
 }
 ```


### Data transformation with side inputs

[@rule.json](usage/side_input.json)
```json
  {
    "When": {
      "Prefix": "/data/case009",
      "Suffix": ".json"
    },
    "Async": true,
    "Dest": {
      "Table": "bqtail.dummy",
      "Transient": {"Dataset": "temp"},
      "Transform": {
        "event_type": "et.name"
      },
      "SideInputs": [
        {
          "Table": "bqtail.event_types",
          "Alias": "et",
          "On": "t.type_id = et.id"
        }
      ]
    },
    "OnSuccess": [
      {
        "Action": "delete"
      }
    ]
  }
```


### Deployment

See [Generic Deployment](../deployment/README.md) automation and post deployment testing