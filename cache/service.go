package cache

import (
	"bqtail/cache/cfs"
	"bytes"
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"io/ioutil"
)

type Service interface {
	Build(context.Context, *Request) *Response
}

type service struct {
	fs afs.Service
}

func (s *service) Build(ctx context.Context, request *Request) *Response {
	response := NewResponse()
	err := s.cache(ctx, request, response)
	response.SetIfError(err)
	return response
}

func (s *service) cache(ctx context.Context, request *Request, response *Response) error {
	objects, err := s.fs.List(ctx, request.URL, option.NewRecursive(true))
	if err != nil {
		return err
	}
	response.Objects = make([]string, 0)
	var items = make([]*cfs.Entry, 0)
	for _, object := range objects {
		if object.IsDir() {
			continue
		}
		response.Objects = append(response.Objects, object.URL())
		reader, err := s.fs.DownloadWithURL(ctx, object.URL())
		if err != nil {
			return err
		}
		data, err := ioutil.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return err
		}

		items = append(items, &cfs.Entry{
			URL:     object.URL(),
			Data:    data,
			ModTime: object.ModTime(),
		})
	}
	entries := &cfs.Cache{
		Items: items,
	}
	JSON, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	cacheURL := cfs.CacheURL(request.URL)
	response.CacheURL = cacheURL
	return s.fs.Upload(ctx, cacheURL, file.DefaultFileOsMode, bytes.NewReader(JSON))
}

func New(fs afs.Service) Service {
	return &service{
		fs: fs,
	}
}
