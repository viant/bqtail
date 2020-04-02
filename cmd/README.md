## BqTail - command line loader

Stand alone Google Storage based BigQuery loader.

### Introduction

BqTail command loader manages ingestion process as stand along process using [Data ingestion rules](../../bqtail/tail/README.md#data-ingestion-rules).
For each source datafile an event is triggered to local BqTail process.
For any source where URL is not Google Storage (gs://), the tail process copies data file to Google Storage bucket followed by triggering local events.
BqTail client supports all feature of serverless BqTail with exception that is always run in sync mode, on top of that it also support constant data streaming option.

When ingesting data bqtail process manages history for all successfully process file, to avoid processing the same file more than once.
By default only streaming mode stores history file in file:///${env.HOME}/.bqtail location, otherwise memory filesystem is used.

### Installation

##### OSX


```bash
wget https://github.com/viant/bqtail/releases/download/v2.1.0/bqtail_osx_2.1.0.tar.gz
tar -xvzf bqtail_osx_2.1.0.tar.gz
cp bqtail /usr/local/bin/
```


##### Linux

```bash
wget https://github.com/viant/bqtail/releases/download/v2.1.0/bqtail_linux_2.1.0.tar.gz
tar -xvzf bqtail_linux_2.1.0.tar.gz
cp bqtail /usr/local/bin/
```

### Usage  

Make sure that you have **temp** dataset in the project.

**Data ingestion rule validation**

To validate rule use -V option.

```bash
bqtail -r='myRuleURL -V' -p=myProject
bqtail -s=mydatafile -d='myProject:mydataset.mytable' -V
bqtail -r=gs://MY_CONFIG_BUCKET/BqTail/Rules/sys/bqjob.yaml -V
```


**Local data file ingestion**

```bash
bqtail -s=mydatafile -d='myProject:mydataset.mytable' -b=myGCSBucket
```

**Google storage file ingestion**


The following line creates default ingestion rule to ingest data directly from Google Storage

```bash
bqtail -s=gs://myBuckey/folder/mydatafile.csv -d='myProject:mydataset.mytable' 
```

The command ingests data to the dest table and produces the following rule:

```yaml
Async: true
Dest:
  Table: myProject:mydataset.mytable
  Transient:
    Alias: t
    Dataset: temp
    ProjectID: myProject
Info:
  LeadEngineer: awitas
  URL: mem://localhost/BqTail/config/rule/rule.yaml
  Workflow: rule
OnSuccess:
- Action: delete
  Request:
    URLs: $LoadURIs
When:
  Prefix: /folder/
```

You can save it as rule.yaml to extend/customize the rule, then you can ingest data with updated rule:

```yaml
bqtail -s=gs://myBuckey/folder/mydatafile.csv -r=rule.yaml
```




**Local data ingestion with data ingestion rule**

```bash
bqtail -s=mydatafile -r='myRuleURL'  -b=myGCSBucket
```

**Local data files ingestion**

```bash
bqtail -s=mylocaldatafolder -d='myProject:mydataset.mytable' -b=myGCSBucket
```

**Local data files ingestion in batch with 120 sec window**

```bash
bqtail -s=mylocaldatafolder -d='myProject:mydataset.mytable' -w=120  -b=myGCSBucket
```

**Local data files streaming ingestion with rule**

```bash
bqtail -s=mylocaldatafolder -r='myRuleURL' -X 
```

**Local data files ingestion in batch with 120 sec window with processed file tracking**

```bash
bqtail -s=mylocaldatafolder -d='myProject:mydataset.mytable' -w=120 -h=~/.bqtail
```


### Authentication

BqTail client can use one the following auth method

1. With BqTail BigQuery OAuth client (by default)

- no env setting needed

2.With Google Service Account Secrets

```bash
export GOOGLE_APPLICATION_CREDENTIALS=myGoogle.secret
```

3. With gsutil authentication

```bash
    gcloud config set project my-project
    gcloud auth login`
    export GCLOUD_AUTH=true
``` 

4. With custom BigQuery Oath clent

-c switch


```bash
bqtail -c=pathTo/custom.json
```

where:
-  @pathTo/custom.json

```json
{
   "Id": "xxxx.apps.googleusercontent.com",
  "Secret": "xxxxxx"

}
```


Help: 

```bash
bqtail -h
```