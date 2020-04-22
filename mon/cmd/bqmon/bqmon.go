package main

import (
	"github.com/viant/bqtail/mon/cmd"
	"os"
)

//Version app version
var Version string

func main() {
	cmd.RunClient(Version, os.Args[1:])
}
