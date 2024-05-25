package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/kushnirko/kpi-apz-lab-4/httptools"
	"github.com/kushnirko/kpi-apz-lab-4/logger"
	"github.com/kushnirko/kpi-apz-lab-4/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
	logEnabled   = flag.Bool("log", true, "whether to write logs to stdout")
)

var (
	mutex       sync.Mutex
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
)

type Server struct {
	address  string
	busyness int
}

func (s *Server) AddReq() {
	mutex.Lock()
	defer mutex.Unlock()
	s.busyness += 1
}

func (s *Server) CloseReq() {
	if s.busyness == 0 {
		log.Println("Failed attempt to close a session that does not exist")
		return
	}
	mutex.Lock()
	defer mutex.Unlock()
	s.busyness -= 1
}

type Servers struct {
	list []*Server
}

func (s *Servers) GetServerIndex(srv Server) (int, error) {
	for i := range s.list {
		if s.list[i].address == srv.address &&
			s.list[i].busyness == srv.busyness {
			return i, nil
		}
	}
	return -1, errors.New("The specified Server does not exist")
}

func (s *Servers) IsServerOnList(srv Server) bool {
	for i := range s.list {
		if s.list[i].address == srv.address &&
			s.list[i].busyness == srv.busyness {
			return true
		}
	}
	return false
}

func (s *Servers) RemoveServerFromList(srv Server) {
	index, err := s.GetServerIndex(srv)
	if err != nil {
		log.Printf("Failed to remove Server: %s", err)
		return
	}
	s.list = append(s.list[:index], s.list[index+1:]...)
}

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

type Health struct {
}

func (h Health) health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		logger.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

type Balancer struct {
	checker          HealthChecker
	checkRate        int
	availableServers Servers
}

type HealthChecker interface {
	health(dst string) bool
}

func (b *Balancer) getAvailableServers(pool []string) {
	for _, addr := range pool {
		availableServer := Server{addr, 0}
		b.availableServers.list = append(b.availableServers.list, &availableServer)
	}
}

func (b *Balancer) selectLessBusyServer() (*Server, error) {
	if len(b.availableServers.list) == 0 {
		return nil, errors.New("Servers are not available")
	}
	lessBusyServer := b.availableServers.list[0]

	for i := range b.availableServers.list {
		if lessBusyServer.busyness > b.availableServers.list[i].busyness {
			lessBusyServer = b.availableServers.list[i]
		}
	}

	return lessBusyServer, nil
}

func (b *Balancer) MonitorServersState() {
	servers := &b.availableServers

	for i := range servers.list {
		availableServer := servers.list[i]
		go func() {
			for range time.Tick(time.Duration(b.checkRate) * time.Second) {
				serverHealthy := b.checker.health(availableServer.address)
				if !serverHealthy && servers.IsServerOnList(*availableServer) {
					servers.RemoveServerFromList(*availableServer)
				}
				if serverHealthy && !servers.IsServerOnList(*availableServer) {
					servers.list = append(servers.list, availableServer)
				}
				logger.Println(availableServer.address, serverHealthy)
			}
		}()
	}
}

func main() {
	flag.Parse()
	logger.Init(*logEnabled)

	Balancer := &Balancer{
		checker:          Health{},
		checkRate:        10,
		availableServers: Servers{},
	}

	Balancer.getAvailableServers(serversPool)
	Balancer.MonitorServersState()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		server, err := Balancer.selectLessBusyServer()
		if err != nil {
			log.Printf("Failed to send a request: %s", err)
			return
		}
		server.AddReq()
		err = forward(server.address, rw, r)
		if err == nil {
			server.CloseReq()
		}
	}))

	logger.Println("Starting load balancer...")
	logger.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
