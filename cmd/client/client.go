package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/kushnirko/kpi-apz-lab-5/logger"
)

var (
	target = flag.String("target", "http://localhost:8090", "request target")

	logEnabled = flag.Bool("log", true, "whether to write logs to stdout")
)

func main() {
	flag.Parse()
	logger.Init(*logEnabled)

	client := new(http.Client)
	client.Timeout = 10 * time.Second

	for range time.Tick(1 * time.Second) {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", *target))
		if err == nil {
			logger.Printf("response %d", resp.StatusCode)
		} else {
			log.Printf("error %s", err)
		}
	}
}
