package batch

import (
	"github.com/viant/afs"
	"github.com/viant/afs/sync"
	"github.com/viant/toolbox"
)

//Group represent batch group
type Group struct {
	*sync.Counter
}

//SetID sets ID
func (g *Group) SetID(ID int) {
	g.Data = ID
}

//ID returns group ID
func (g *Group) ID() int {
	return toolbox.AsInt(g.Data)
}

//NewGroup creates a group
func NewGroup(URL string, fs afs.Service) *Group {
	return &Group{
		Counter: sync.NewCounter(URL, fs),
	}
}
