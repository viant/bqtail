package cfs

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/mem"
	"github.com/viant/afs/object"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
	"io"
	"strings"
	"sync/atomic"
	"time"
)

type service struct {
	useCache int32
	bucket   string
	URL      string
	next     *time.Time
	modified *time.Time
	afs.Service
}

func (s *service) canUseCache() bool {
	return atomic.LoadInt32(&s.useCache) == 1
}

func (s *service) Download(ctx context.Context, object storage.Object, options ...storage.Option) (io.ReadCloser, error) {
	return s.DownloadWithURL(ctx, object.URL(), options...)
}

func (s *service) DownloadWithURL(ctx context.Context, URL string, options ...storage.Option) (io.ReadCloser, error) {
	err := s.reloadIfNeeded(ctx)
	if err != nil {
		return nil, err
	}
	if !s.canUseCache() {
		return s.Service.DownloadWithURL(ctx, URL, options...)
	}
	cacheURL := strings.Replace(URL, gs.Scheme, mem.Scheme, 1)
	reader, err := s.Service.DownloadWithURL(ctx, cacheURL, options...)
	if err == nil {
		return reader, err
	}
	return s.Service.DownloadWithURL(ctx, URL, options...)
}

func (s *service) rewriteURL(objects []storage.Object) []storage.Object {
	var result = make([]storage.Object, 0)
	for i := range objects {
		obj := objects[i]
		URL := strings.Replace(obj.URL(), mem.Scheme, gs.Scheme, 1)
		URL = strings.Replace(URL, "localhost", s.bucket, 1)
		result = append(result, object.New(URL, obj, obj.Sys()))
	}
	return result
}

func (s *service) List(ctx context.Context, URL string, options ...storage.Option) ([]storage.Object, error) {
	err := s.reloadIfNeeded(ctx)
	if err != nil {
		return nil, err
	}
	if !s.canUseCache() {
		return s.Service.List(ctx, URL, options...)
	}
	cacheURL := strings.Replace(URL, gs.Scheme, mem.Scheme, 1)
	if objects, _ := s.Service.List(ctx, cacheURL, options...); len(objects) > 0 {
		return s.rewriteURL(objects), nil
	}
	return s.Service.List(ctx, URL, options...)
}

func (s *service) setNextRun(next time.Time) {
	s.next = &next
}

func (s *service) reloadIfNeeded(ctx context.Context) error {
	if s.next != nil && s.next.Before(time.Now()) {
		return nil
	}
	s.setNextRun(time.Now().Add(time.Second))
	cacheObject, _ := s.Service.Object(ctx, s.URL)
	if cacheObject == nil {
		atomic.StoreInt32(&s.useCache, 0)
		return nil
	}
	atomic.CompareAndSwapInt32(&s.useCache, 0, 1)
	if s.modified != nil && s.modified.Equal(cacheObject.ModTime()) {
		return nil
	}
	reader, err := s.Service.DownloadWithURL(ctx, s.URL)
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.Close()
	}()
	cache := &Cache{}
	if err = json.NewDecoder(reader).Decode(cache); err != nil {
		return err
	}
	for _, item := range cache.Items {
		URL := strings.Replace(item.URL, gs.Scheme, mem.Scheme, 1)
		if err = s.Service.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(item.Data), item.ModTime); err != nil {
			break
		}
	}
	modTime := cacheObject.ModTime()
	s.modified = &modTime
	return err
}

func New(URL string, fs afs.Service) afs.Service {
	return &service{
		bucket:  url.Host(URL),
		URL:     CacheURL(URL),
		Service: fs,
	}
}
