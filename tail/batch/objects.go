package batch

import (
	"fmt"
	"github.com/viant/afs/storage"
	"strings"
	"time"
)

type Objects []storage.Object

// Len is the number of elements in the collection.
func (o Objects) Len() int {
	return len(o)
}

// Swap swaps the elements with indexes i and j.
func (o Objects) Swap(i, j int) {
	tmp := o[i]
	o[i] = o[j]
	o[j] = tmp
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (o Objects) Less(i, j int) bool {
	return o[i].ModTime().Before(o[j].ModTime())
}

//Lookup return matched object with specified event or error
func (o Objects) Lookup(eventId string) (storage.Object, error) {
	name := eventId + transferableExtension
	for i := range o {
		if o[i].Name() == name {
			return o[i], nil
		}
	}
	return nil, fmt.Errorf("failed to lookup %v", name)
}


func (o Objects) Before(target storage.Object) Objects {
	var result = make([]storage.Object, 0)
	for i := range o {
		if o[i].ModTime().Equal(target.ModTime()) {
			if strings.Compare(o[i].Name(), target.Name()) < 0 {
				result = append(result, o[i])
			}
			continue
		}
		if o[i].ModTime().Before(target.ModTime()) {
			result = append(result, o[i])
			continue
		}
		break
	}
	return result
}


func (o Objects) After(target time.Time) Objects {
	var result = make([]storage.Object, 0)
	for i:= len(o)-1;i>=0;i-- {
		if o[i].ModTime().After(target) {
			result = append(result, o[i])
			continue
		}
		break
	}
	return result
}