package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/bqtail/dispatch"
	"github.com/viant/bqtail/dispatch/contract"
	"log"
	"os"
	"time"
)

func main() {
	config := os.Getenv("CONFIG")
	if config == "" {
		panic("env.CONFIG was empty\n")
	}
	timeout := os.Getenv("FUNCTION_TIMEOUT_SEC")
	if timeout == "" {
		panic("env.FUNCTION_TIMEOUT_SEC was empty\n")
	}

	for {
		_, err := handleDispatchEvent(context.Background())
		if err != nil {
			log.Printf("error: %v\n", err)
		}
		time.Sleep(100 * time.Minute)
	}
}

func handleDispatchEvent(ctx context.Context) (*contract.Response, error) {
	service, err := dispatch.Singleton(ctx)
	if err != nil {
		return nil, err
	}
	response := service.Dispatch(ctx)
	response.Lock()
	defer response.UnLock()
	data, _ := json.Marshal(response)
	fmt.Printf("%s\n", data)
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	return response, nil
}
