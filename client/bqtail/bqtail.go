package main

import (
	"github.com/viant/bqtail/client"
	_ "github.com/viant/afsc/s3"
	"os"
)

var Version string

func main() {
	client.RunClient(Version, os.Args[1:])
}
