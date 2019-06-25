package bq

import (
	"bqtail/gcp"
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

var bigQueryScope = "https://www.googleapis.com/auth/bigquery"
var bigQueryInsertScope = "https://www.googleapis.com/auth/bigquery.insertdata"

//BigQueryClient represents a big query client
type BigQueryClient struct {
	*bigquery.Service
	Context context.Context
}

//NewBigQueryClient create a new big query client
func NewBigQueryClient() (*BigQueryClient, error) {
	httpClient, ctx, err := gcp.NewHttpClient(bigQueryScope, bigQueryInsertScope)
	if err != nil {
		return nil, fmt.Errorf("failed to create auth client: %v", err)
	}
	service, err := bigquery.NewService(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery service: %v", err)
	}
	return &BigQueryClient{
		Service: service,
		Context: ctx,
	}, nil
}
