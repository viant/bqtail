package cmd


type Options struct {
	Version bool `short:"v" long:"version" description:"bqtail version"`

	ProjectID string `short:"p" long:"project" description:"Google Cloud Project"`

	Bucket string `short:"b" long:"bucket" description:"Google Cloud Storage ingestion bucket"`

	Method string `short:"m" long:"bucket" description:"Google Cloud Storage ingestion bucket"`

	SourceURL string `short:"s" long:"src" description:"source URL" `

	TriggerBucket string `short:"s" long:"src" description:"source URL" `

	OperationBucket string `short:"s" long:"src" description:"source URL" `

	DryRun bool `short:"d" long:"dry" description:"dry run flag" `

	Client string `short:"c" long:"client" description:"GCP OAuth client url"`

}

