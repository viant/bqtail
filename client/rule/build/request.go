package build


//Request represents build rule request
type Request struct {
	ProjectID string
	Bucket    string
	BasePath  string
	Window    int
	SourceURL string
}

