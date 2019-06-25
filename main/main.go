package main

import (
	"bqtail"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/url"
	"log"
	"os"
	"path"
)

func main() {

	parent := toolbox.CallerDirectory(3)
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

	configURL := url.NewResource(path.Join(parent, "config.json")).URL
	os.Setenv("CONFIG", configURL)
	config, err := bqtail.NewConfig("CONFIG")

	config.Init()
	err = config.Validate()
	if err != nil {
		log.Fatal(err)
	}
	service := bqtail.New(config)
	//{"EventID":"","SourceURL":"gs://e2e-data/data/case1/dummy.json"}
	response := service.Tail(&bqtail.Request{
		SourceURL: url.NewResource("gs://sitelist/matcher/app00001.avro").URL,
	})

	toolbox.Dump(response)
}
