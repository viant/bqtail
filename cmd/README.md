## BqTail - command line loader

Stand alone Google Storage based BigQuery loader.

### Introduction

BqTail command loader manages ingestion process as stand along process using [Data ingestion rules](../../bqtail/tail/README.md#data-ingestion-rules).
For each source datafile an event is triggered to local BqTail process. 
Since BigQuery Load API accept URI that are valid Google Cloud Storage location, all data events also have to valid GCS locations.

Data event can be **trigger directly** to the bqtail process if source URL is valid Google Cloud Storage URL and source path matches bucket and rule filter.
Otherwise all files are copied from sourceURL to gs://${bucket}/$filterPath, and then event is fired.

In **direct eventing mode** all data files are govern by BqTail ingestion rule. For example if rule uses batching window, 
datafile last modification is used to allocate corresponding batches. 
Take another example when a rule uses delete action on Success, all matched file would be deleted. 

For non direct mode, original data files are never deleted, to avoid the same file processing between a separate
bqtail commands run, you can use -h or -X parameter to store all successfully processed file in a history file.

By default only streaming mode stores history file in file:///${env.HOME}/.bqtail location, otherwise memory filesystem is used.

### Installation

##### OSX


```bash
wget https://github.com/viant/bqtail/releases/download/v2.1.1/bqtail_osx_2.1.1.tar.gz
tar -xvzf bqtail_osx_2.1.1.tar.gz
cp bqtail /usr/local/bin/
```


##### Linux

```bash
wget https://github.com/viant/bqtail/releases/download/v2.1.1/bqtail_linux_2.1.1.tar.gz
tar -xvzf bqtail_linux_2.1.1.tar.gz
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