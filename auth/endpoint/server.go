package endpoint

import (
	"fmt"
	"github.com/pkg/errors"
	"net"
	"net/http"
	"strings"
)

//Server represent an auth callback endpoint
type Server struct {
	Port     int
	err      error
	listener net.Listener
	server   *http.Server
	*httpHandler
}

//Close closes server
func (s *Server) Close() {
	s.httpHandler.done <- true
}

//Wait waits
func (s *Server) Wait() error {
	select {
	case <-s.httpHandler.done:
		s.server.Close()
	}
	return s.err
}

//Start starts a server
func (s *Server) Start() {
	err := s.server.Serve(s.listener)
	if err != nil {
		if !strings.Contains(err.Error(), "closed") {
			s.httpHandler.done <- true
			return
		}
		s.err = err
	}
}

//New creates an auth callback endpoint
func New() (*Server, error) {
	result := &Server{httpHandler: newHttpHandler()}

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, errors.Wrapf(err, "failt to create http endpoint")
	}
	result.listener = listener
	result.Port = listener.Addr().(*net.TCPAddr).Port
	result.server = &http.Server{Addr: fmt.Sprintf(":%v", result.Port), Handler: result.httpHandler}
	return result, nil
}
