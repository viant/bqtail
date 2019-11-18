package batch

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/object"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"sort"
	"testing"
	"time"
)

func newObject(name string, time time.Time) storage.Object {
	resource := asset.NewFile(name, []byte("1"), 0655)
	resource.ModTime = &time
	URL := url.Join("mem://localhost/", name)
	return object.New(URL, resource.Info(), resource)
}

func TestObjects_Swap(t *testing.T) {

	now := time.Now()

	var useCases = []struct {
		description string
		objects     []storage.Object
		by func(o1, o2 storage.Object) bool
		expect []string
	}{
		{
			description: "different time",
			objects: []storage.Object{
				newObject("test0001", now.Add(-2*time.Millisecond)),
				newObject("test0002", now.Add(-1*time.Millisecond)),
			},
			by: byModTime,
			expect: []string{"test0001", "test0002"},
		},
		{
			description: "different time - resort",
			objects: []storage.Object{
				newObject("test0002", now.Add(-1*time.Millisecond)),
				newObject("test0001", now.Add(-2*time.Millisecond)),
			},
			by: byModTime,
			expect: []string{"test0001", "test0002"},
		},
		{
			description: "the same time time",
			objects: []storage.Object{
				newObject("test0002", now.Add(-3*time.Millisecond)),
				newObject("test0001", now.Add(-3*time.Millisecond)),
			},
			by: byModTime,
			expect: []string{"test0001", "test0002"},
		},
		{
			description: "by name",
			objects: []storage.Object{
				newObject("1574024639643000000.win", now.Add(-7*time.Millisecond)),
				newObject("1574024639584000000.win", now.Add(-2*time.Millisecond)),
				newObject("1574024639577000000.win", now.Add(-3*time.Millisecond)),

			},
			by: byName,
			expect: []string{"1574024639577000000.win", "1574024639584000000.win", "1574024639643000000.win"},
		},
	}

	for _, useCase := range useCases {
		sortable := NewObjects(useCase.objects, useCase.by)
		sort.Sort(sortable)
		var actual = make([]string, 0)
		for i := range sortable.Elements {
			actual = append(actual, sortable.Elements[i].Name())
		}
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}
}
