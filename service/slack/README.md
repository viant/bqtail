# Slack service

The following action are supported:

#### notify

1. Notify example with slack oauth token encrypted with GCP KMS

```json
{
  "Action": "notify",
  "Request": {
    "Channels": [
      "#e2e"
    ],
    "From": "bqtail",
    "Title": "bqtail.wrong_dummy ingestion",
    "Message": "$Error",
    "Secret": {
      "URL": "gs://${config.Bucket}/config/slack.json.enc",
      "Key": "bqtailRing/BqTailKey"
    }
  }
}
```

See how to [Securing slack](https://github.com/viant/bqtail/tree/master/deployment#credentials-setup).

2. Notify example with inline token 

```json
{
  "Action": "notify",
  "Request": {
    "Channels": [
      "#e2e"
    ],
    "From": "bqtail",
    "Title": "bqtail.wrong_dummy ingestion",
    "Message": "$Error",
    "Token": "myslack token"
  }
}
```


where request should be compatible with the following type:

```go
type NotifyRequest struct {
	Channels []string
	From     string
	Title    string
	Message  string
	Body     interface{}
	Secret   *base.Secret
	BodyType string
	OAuthToken
}

```

In addition slack can use the following variables:

- $DestTable: destination table
- $TempTable: temp table
- $EventID: storage event id triggering load or batch
- $URLs: coma separated list of load URIs
- $SourceURI: one of load URI
- $RuleURL: transfer rule URL
- $Error - error message
