package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kushnirko/kpi-apz-lab-5/httptools"
	"github.com/kushnirko/kpi-apz-lab-5/logger"
	"github.com/kushnirko/kpi-apz-lab-5/signal"
)

var (
	port = flag.Int("port", 8080, "server port")

	logEnabled = flag.Bool("log", true, "whether to write logs to stdout")
)

const (
	confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
	confHealthFailure    = "CONF_HEALTH_FAILURE"

	database = "http://database:8080/db"
	teamName = "ryan-gosling-team"
)

func sendTimestamp() {
	body := new(bytes.Buffer)
	json.NewEncoder(body).Encode(struct {
		Value string `json:"value"`
	}{
		Value: time.Now().Format("2006-01-02"),
	})

	c := http.DefaultClient
	resp, err := c.Post(fmt.Sprintf("%s/%s", database, teamName), "application/json", body)
	if err != nil {
		log.Printf("failed to send timestamp: %v", err)
	}
	resp.Body.Close()
}

func main() {
	flag.Parse()
	logger.Init(*logEnabled)

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		k := r.URL.Query().Get("key")
		if k == "" {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}

		c := http.DefaultClient
		resp, err := c.Get(fmt.Sprintf("%s/%s", database, k))
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(resp.StatusCode)

		if resp.StatusCode == http.StatusOK {
			if _, err = io.Copy(rw, resp.Body); err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
			}
		}
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	sendTimestamp()
	signal.WaitForTerminationSignal()
}
