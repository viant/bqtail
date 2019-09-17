# Big Query service

The following action are supported:

#### export

Export extract table to google storage.
When use with dispatch service, source value is populated from event destination table.


```json
{
   "Action": "copy",
   "Request": {
      "Source": "mydataset:source_table",
      "Dest": "mydataset.dest_table"
   }
}
```



#### copy

Copy source to destination
When use with dispatch service, source value is populated from event destination table.

```json
{
   "Action": "copy",
   "Request": {
      "Source": "mydataset:source_table",
      "Dest": "mydataset.dest_table"
   }
}
```



#### query

Query run supplied SQL

```json
{
   "Action": "query",
   "Request": {
      "SQL" :"SELECT '$JobID' AS job_id, COUNT(1) AS row_count, CURRENT_TIMESTAMP() AS completed FROM $DestTable",
      "Append": true,
      "Dest": "mydataset.ingestion_summary"
   }
}
```

where request should be compatible with the following type:


```go
type QueryRequest struct {
	DatasetID string
	SQL       string
	UseLegacy bool
	Append    bool
	Dest      string
}
```
