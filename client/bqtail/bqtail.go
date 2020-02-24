package main

import (
	"github.com/viant/bqtail/client"
	"os"
)

func main() {
	client.RunClient(os.Args[1:])
}
