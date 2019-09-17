# Slack service

The following action are supported:

#### notify

```json
{
  "Action": "notify",
  "Request": {
    "Channels": [
      "#e2e"
    ],
    "From": "BqTail",
    "Title": "bqtail.wrong_dummy ingestion",
    "Message": "$Error",
    "Secret": {
      "URL": "gs://${config.Bucket}/config/slack.json.enc",
      "Key": "BqTailRing/BqTailKey"
    }
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
