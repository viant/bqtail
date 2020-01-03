# HTTP service

The following action are supported:

#### call

1. Call example with post body defined in separate file. 

```json
 {
        "Action": "call",
        "Request": {
          "BodyURL": "gs://myPath/body.txt",
          "URL": "$MyCloudFunctionEndpoty",
          "Method": "POST",
          "Auth": true
        }
}
```



where request should be compatible with the following type:

[CallRequest](call.go#L61)
```go
//CallRequest represents an http call request
type CallRequest struct {
	URL     string
	Method  string
	BodyURL string
	Body    string
	//Auth authenticate to call non public cloud function
	Auth bool
	//Scopes for OAuth HTTP client
	Scopes []string
}

```

In addition call task can use the following variables

- $DestTable: destination table
- $TempTable: temp table
- $EventID: storage event id triggering load or batch
- $URLs: coma separated list of load URIs
- $SourceURI: one of load URI
- $RuleURL: transfer rule URL
