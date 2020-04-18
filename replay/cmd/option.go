package cmd


type Options struct {

	Version bool `short:"v" long:"version" description:"bqtail version"`

	ProjectID string `short:"p" long:"project" description:"Google Cloud Project"`

	Method string `short:"m" long:"method" choice:"data" choice:"proc" description:"replay based on failed process or stalled datafiles"`

	Age  string `short:"a" long:"age" description:"min unprocess datafile age expression like: 60 min"`

	SourceURL string `short:"s" long:"src" description:"source URL" `

	TriggerBucket string `short:"t" long:"src" description:"source URL" `

	DryRun bool `short:"d" long:"dry" description:"dry run flag" `

	Client string `short:"c" long:"client" description:"GCP OAuth client url"`

}
