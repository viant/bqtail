package batch

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"io/ioutil"
	"time"
)

//Datafile represents transfer data file
type Datafile struct {
	URL       string    `json:",omitempty"`
	EventID   string    `json:",omitempty"`
	SourceURL string    `json:",omitempty"`
	Created   time.Time `json:",omitempty"`
}


func loadDatafile(ctx context.Context, object storage.Object, fs afs.Service) (*Datafile, error) {
	reader, err := fs.Download(ctx, object)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	_, name := url.Split(object.URL(), file.Scheme)
	name = string(name[:len(name)-4])
	return &Datafile{SourceURL: string(data), EventID: name, Created: object.ModTime(), URL: object.URL()}, nil
}
