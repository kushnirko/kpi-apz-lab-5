package main

import (
	"encoding/json"
	"net/http"

	"github.com/kushnirko/kpi-apz-lab-5/logger"
)

const reportMaxLen = 100

type Report map[string][]string

func (r Report) Process(req *http.Request) {
	author := req.Header.Get("lb-author")
	counter := req.Header.Get("lb-req-cnt")
	logger.Printf("GET some-data from [%s] request [%s]", author, counter)

	if len(author) > 0 {
		list := r[author]
		list = append(list, counter)
		if len(list) > reportMaxLen {
			list = list[len(list)-reportMaxLen:]
		}
		r[author] = list
	}
}

func (r Report) ServeHTTP(rw http.ResponseWriter, _ *http.Request) {
	rw.Header().Set("content-type", "application/json")
	rw.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(rw).Encode(r)
}
