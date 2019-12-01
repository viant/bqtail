package cfs

import "github.com/viant/afs"

var singleton afs.Service

//Singleton returns singleton
func Singleton(URL string) afs.Service {
	if singleton != nil {
		return singleton
	}
	singleton = New(URL, afs.New())
	return singleton
}
