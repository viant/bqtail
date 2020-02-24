## BqTail - client

Stand alone Google Storage based BigQuery loader.

### Introduction

BqTail client manages ingestion process on client side using [Data ingestion rules](../../bqtail/tail/README.md#data-ingestion-rules).
For each source datafile an event is triggered to local BqTail process.
For any source where URL is not Google Storage (gs://), the tail process copies data file to Google Storage bucket followed by triggering local events.
BqTail client supports all feature of serverless BqTail with exception that is always run in sync mode, on top of that it also support constant data streaming option.

When ingesting data bqtail process manages history for all successfully process file, to avoid processing the same file more than once.
By default only streaming mode stores history file in file:///${env.HOME}/.bqtail location, otherwise memory filesystem is used.

### Installation




### Usage  

Help: 

```bash
bqtail -h
```

**Data ingestion rule validation**

To validate rule use -V option.

```bash
    export GOOGLE_APPLICATION_CREDENTIALS='mygoogle-secret.json'

    bqtail -r='myRuleURL -V'

   bqtail -s=mydatafile -d='myProject:mydataset.mytable' -V
  
```


**Local data file ingestion**

```bash
    export GOOGLE_APPLICATION_CREDENTIALS='mygoogle-secret.json'
    bqtail -s=mydatafile -d='myProject:mydataset.mytable'
```




**Local data ingestion with data ingestion rule**

```bash
    export GOOGLE_APPLICATION_CREDENTIALS='mygoogle-secret.json'
    bqtail -s=mydatafile -r='myRuleURL' 
```

**Local data files ingestion**

```bash
    export GOOGLE_APPLICATION_CREDENTIALS='mygoogle-secret.json'
    bqtail -s=mylocaldatafolder -d='myProject:mydataset.mytable'
```

**Local data files ingestion in batch with 120 sec window**

```bash
    export GOOGLE_APPLICATION_CREDENTIALS='mygoogle-secret.json'
    bqtail -s=mylocaldatafolder -d='myProject:mydataset.mytable' -w=120
```

**Local data files streaming ingestion with rule**

```bash
    export GOOGLE_APPLICATION_CREDENTIALS='mygoogle-secret.json'
    bqtail -s=mylocaldatafolder -r='myRuleURL' -X 
```

**Local data files ingestion in batch with 120 sec window with processed file tracking**

```bash
    export GOOGLE_APPLICATION_CREDENTIALS='mygoogle-secret.json'
    bqtail -s=mylocaldatafolder -d='myProject:mydataset.mytable' -w=120 -h=~/.bqtail
```
