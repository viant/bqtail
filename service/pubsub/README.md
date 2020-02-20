# HTTP service

The following action are supported:

#### call

1. Push example with message object  

```yaml
Async: true
When:
  Prefix: "/data/case${parentIndex}/"
  Suffix: ".json"
Dest:
  Table: bqtail.dummy_v${parentIndex}
OnSuccess:
  - Action: push
    Request:
      Topic: ${prefix}_bqtailbus
      Attributes:
        EventID: $EventID
        CaseNo: '${parentIndex}'
      Message:
        RuleURL: $RuleURL
        SourceURIs: $LoadURIs
        URLs: $URLs

  - Action: delete
```



where request should be compatible with the following type:

[PushRequest](publish.go#L61)
```go
//PushRequest pubsub push request
type PushRequest struct {
	ProjectID  string
	Topic      string
	Data       []byte
	Message    interface{}
	Attributes map[string]interface{}
}
```

In addition push task can use the following variables

- $DestTable: destination table
- $TempTable: temp table
- $EventID: storage event id triggering load or batch
- $URLs: coma separated list of load URIs
- $SourceURI: one of load URI
- $RuleURL: transfer rule URL
- Pattern Parameters