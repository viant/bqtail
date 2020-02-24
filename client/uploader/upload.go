package uploader

//OnDone represents on upload done callback
type OnDone func(URL string, err error)

//Request represent an upload
type Request struct {
	src  string
	dest string
}

//NewRequest creates a new upload request
func NewRequest(src, dest string) *Request {
	return &Request{src: src, dest: dest}
}
