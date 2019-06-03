package gcp

import (
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

type BigQueryService struct {
	*bigquery.Service
	Context context.Context
}

func NewBigQueryClient(scopes ...string) (*BigQueryService, error) {
	httpClient, ctx, err := NewHttpClient(scopes...)
	if err != nil {
		return nil, fmt.Errorf("failed to create auth client: %v", err)
	}
	service, err := bigquery.NewService(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, fmt.Errorf("failed to create bigquery service: %v", err)
	}
	return &BigQueryService{
		Service: service,
		Context: ctx,
	}, nil
}
