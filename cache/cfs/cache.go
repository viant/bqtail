package cfs

import (
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
	"path"
)

type Cache struct {
	Items []*Entry
}

//CacheURL returns a cache URL
func CacheURL(URL string) string {
	if path.Ext(URL) != "" {
		URL, _ = url.Split(URL, gs.Scheme)
	}
	return url.Join(URL, "_.cache")
}
