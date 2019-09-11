package dispatch



//
//func TestService_Run(t *testing.T) {
//
//	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path.Join(os.Getenv("HOME"), ".secret/gcp-e2e.json"))
//	parent := toolbox.CallerDirectory(3)
//
//
//
//
//	configURL := path.Join(parent, "../e2e/config/bqdispatch.json")
//	ctx := context.Background()
//	config, err := NewConfigFromURL(ctx, configURL)
//	if err != nil {
//		log.Fatal(err)
//	}
//	srv, err := New(ctx, config)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	response := srv.Dispatch(ctx, &contract.Request{
//		ProjectID:"viant-e2e",
//		JobID:"BQTail_Job_abc121023123-2",
//	})
//
//	toolbox.Dump(response )
//}