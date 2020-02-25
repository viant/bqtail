package main

import (
	_ "github.com/viant/afsc/s3"
	"github.com/viant/bqtail/cmd"
	"os"
)

var Version string

func main() {
	cmd.RunClient(Version, os.Args[1:])
}
