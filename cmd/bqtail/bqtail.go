package main

import (
	"github.com/viant/bqtail/cmd"
	_ "github.com/viant/afsc/s3"
	"os"
)

var Version string

func main() {
	cmd.RunClient(Version, os.Args[1:])
}
