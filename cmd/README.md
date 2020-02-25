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

wget https://github.com/viant/bqtail/releases/download/v2.0.0/bqtail_osx_2.0.0.tar.gz
tar -xvzf bqtail_osx_2.0.0.tar.gz
cp bqtail /usr/local/bin/

##### Linux

wget https://github.com/viant/bqtail/releases/download/v2.0.0/bqtail_linux_2.0.0.tar.gz
tar -xvzf bqtail_linux_2.0.0.tar.gz
cp bqtail /usr/local/bin/


### Usage  

BqTail client can use one the following auth method

1.With Google Service Account Secrets

```bash
export GOOGLE_APPLICATION_CREDENTIALS=myGoogle.secret
```

2. With gsutil authentication

```bash
    gcloud config set project my-project
    gcloud auth login`
    export GCLOUD_AUTH=true
``` 

3. With BqTail client




Help: 

```bash
bqtail -h
```

**Data ingestion rule validation**

To validate rule use -V option.

```bash
bqtail -r='myRuleURL -V' -p=myProject
bqtail -s=mydatafile -d='myProject:mydataset.mytable' -V
```

**Local data file ingestion**

```bash
bqtail -s=mydatafile -d='myProject:mydataset.mytable'
```

**Local data ingestion with data ingestion rule**

```bash
bqtail -s=mydatafile -r='myRuleURL' 
```

**Local data files ingestion**

```bash
bqtail -s=mylocaldatafolder -d='myProject:mydataset.mytable'
```

**Local data files ingestion in batch with 120 sec window**

```bash
bqtail -s=mylocaldatafolder -d='myProject:mydataset.mytable' -w=120
```

**Local data files streaming ingestion with rule**

```bash
bqtail -s=mylocaldatafolder -r='myRuleURL' -X 
```

**Local data files ingestion in batch with 120 sec window with processed file tracking**

```bash
bqtail -s=mylocaldatafolder -d='myProject:mydataset.mytable' -w=120 -h=~/.bqtail
```
