package httptools

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/kushnirko/kpi-apz-lab-5/logger"
)

type Server interface {
	Start()
}

type server struct {
	httpServer *http.Server
}

func (s server) Start() {
	go func() {
		logger.Println("Staring the HTTP server...")
		err := s.httpServer.ListenAndServe()
		log.Fatalf("HTTP server finished: %s. Finishing the process.", err)
	}()
}

func CreateServer(port int, handler http.Handler) Server {
	return server{
		httpServer: &http.Server{
			Addr:           fmt.Sprintf(":%d", port),
			Handler:        handler,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		},
	}
}
