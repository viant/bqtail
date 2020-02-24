package main

import (
	"github.com/viant/bqtail/client"
	_ "github.com/viant/afsc/s3"
	"os"
)

func main() {
	client.RunClient(os.Args[1:])
}
