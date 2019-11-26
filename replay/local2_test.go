package replay

import (
	"context"
	_ "github.com/viant/afsc/gs"
	"github.com/viant/toolbox"
	"os"
	"path"
	"testing"
)

func TestService_Replay3(t *testing.T) {

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path.Join(os.Getenv("HOME"), ".secret", "viant-dataflow.json"))
	service := New()

	request := &Request{
		//TriggerURL: "gs://viant_dataflow_bqtail/unison/",
		TriggerURL:          "gs://viant_dataflow_bqtail/nobid/adlog.request/2019/11/26/01",
		ReplayBucket:        "viant_dataflow_bqtail/_replay_",
		UnprocessedDuration: "60min",
	}

	response := service.Replay(context.Background(), request)
	_ = toolbox.DumpIndent(response, true)

	//for i :=8;i<20;i++ {
	//	fmt.Printf("cycke: %02d\n", i)
	//
	//
	//	// gs://viant_dataflow_bqtail/gcs-logging/PROJECT_formal-cascade-571_BUCKET_xumo_bucket_2015_usage_2019_11_15_22_00_00_04b0761c59fcb6270b_v0
	//	request := &Request{
	//		//	TriggerURL:          "gs://viant_dataflow_bqtail/nobid/adlog.request/2019/11/",
	//
	//		TriggerURL:         fmt.Sprintf( "gs://viant_dataflow_bqtail/unison/impressions/us-east4/2019/11/17/%02d",i),
	//		ReplayBucket:        "viant_dataflow_bqtail/_replay_",
	//		UnprocessedDuration: "30min",
	//	}
	//	response := service.Replay(context.Background(), request)
	//	_ = toolbox.DumpIndent(response, true)
	//
	//	time.Sleep(6 * time.Minute)
	//}
}
