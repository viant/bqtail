package mon


import (
	"github.com/viant/afs/storage"
)

type Objects struct {
	Elements []storage.Object
	By func(o1, o2 storage.Object) bool
}

// Len is the number of elements in the collection.
func (o Objects) Len() int {
	return len(o.Elements)
}

// Swap swaps the elements with indexes i and j.
func (o Objects) Swap(i, j int) {
	tmp := o.Elements[i]
	o.Elements[i] = o.Elements[j]
	o.Elements[j] = tmp
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (o Objects) Less(i, j int) bool {
	return o.By(o.Elements[i], o.Elements[j])
}


func byName(o1, o2 storage.Object) bool {
	return o1.Name() < o2.Name()
}

func byModTime(o1, o2 storage.Object) bool {
	if o1.ModTime().Equal(o2.ModTime()) {
		return o1.Name() < o2.Name()
	}
	return o1.ModTime().Before(o2.ModTime())
}

func NewObjects(objects []storage.Object, by func(o1, o2 storage.Object) bool) *Objects{
	return &Objects{
		Elements:objects,
		By:by,
	}
}
