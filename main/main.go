package main

import (
	"bqtail"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/url"
	"log"
	"os"
)

func main() {

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/Users/awitas/.secret/viant-e2e.json")
	//
	//config := &bqtail.Config{}
	//config.Rules = model.NewRules("", &model.Rule{
	//	Source:model.Resource{Ext:".avro"},
	//	Dest:model.Table{
	//		TableID:"site_list_match",
	//		DatasetID:"sitemgnt",
	//	},
	//})

	os.Setenv("k1", "gs://e2e-data/config/bqtail.json")
	config, err := bqtail.NewConfig("k1")

	config.Init()
	err = config.Validate()
	if err != nil {
		log.Fatal(err)
	}
	service := bqtail.New(config)
	//{"EventID":"","SourceURL":"gs://e2e-data/data/case1/dummy.json"}
	response := service.Tail(&bqtail.Request{
		SourceURL: url.NewResource("gs://e2e-data/data/case2/dummy.json").URL,
	})

	toolbox.Dump(response)
}
