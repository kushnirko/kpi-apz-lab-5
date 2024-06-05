package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/kushnirko/kpi-apz-lab-5/datastore"
	"github.com/kushnirko/kpi-apz-lab-5/httptools"
	"github.com/kushnirko/kpi-apz-lab-5/logger"
	"github.com/kushnirko/kpi-apz-lab-5/signal"
)

var (
	port        = flag.Int("port", 8080, "server port")
	path        = flag.String("from", "", "recover database from disk")
	temp        = flag.Bool("temp", false, "create temporary database")
	segmentSize = flag.Int("segment", 10*1024*1024, "size of database segment")

	logEnabled = flag.Bool("log", true, "whether to write logs to stdout")
)

func createDirectory() (string, error) {
	switch {
	case *path != "" && *temp:
		return "", fmt.Errorf("cannot use [from] and [temp] together")
	case *path != "":
		return *path, nil
	case *temp:
		return os.MkdirTemp("", "temp-db")
	default:
		dir := "./new-db"
		if err := os.Mkdir(dir, 0o700); err != nil {
			return "", err
		}
		return dir, nil
	}
}

func parseUrl(url *url.URL) (k, t string, err error) {
	pathParts := strings.Split(url.EscapedPath(), "/")
	query := url.Query()
	if len(pathParts) != 3 || len(query) > 1 {
		return "", "", fmt.Errorf("invalid url: %s", url)
	}

	k = pathParts[2]
	t = query.Get("type")
	if t == "" {
		t = "string"
	}
	return
}

type responseBody struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type requestBody struct {
	Value string `json:"value"`
}

func handleGetRequest(rw http.ResponseWriter, r *http.Request, db *datastore.Db) {
	k, t, err := parseUrl(r.URL)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	var v any
	switch t {
	case "string":
		v, err = db.Get(k)
	case "int64":
		v, err = db.GetInt64(k)
	default:
		http.Error(rw, fmt.Sprintf("invalid data type %s", t), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(rw, fmt.Sprintf("no value found for key %s", k), http.StatusNotFound)
		return
	}

	rw.Header().Set("content-type", "application/json")
	if err = json.NewEncoder(rw).Encode(responseBody{Key: k, Value: v}); err != nil {
		http.Error(rw, "json encoding error", http.StatusInternalServerError)
	}
}

func handlePostRequest(rw http.ResponseWriter, r *http.Request, db *datastore.Db) {
	k, t, err := parseUrl(r.URL)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}

	var rb requestBody
	if err = json.NewDecoder(r.Body).Decode(&rb); err != nil {
		http.Error(rw, "json decoding error", http.StatusBadRequest)
		return
	}

	switch t {
	case "string":
		err = db.Put(k, rb.Value)
	case "int64":
		var v int64
		v, err = strconv.ParseInt(rb.Value, 10, 64)
		if err == nil {
			err = db.PutInt64(k, v)
		}
	default:
		err = fmt.Errorf("invalid data type %s", t)
	}
	if err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	rw.WriteHeader(http.StatusCreated)
}

func main() {
	flag.Parse()
	logger.Init(*logEnabled)

	dir, err := createDirectory()
	if err != nil {
		log.Fatal(err)
	}

	db, err := datastore.NewDb(dir, *segmentSize)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = db.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	h := new(http.ServeMux)
	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {

		log.Println(r.URL.Path)
		log.Println(r.Method)

		switch r.Method {
		case "GET":
			handleGetRequest(rw, r, db)
		case "POST":
			handlePostRequest(rw, r, db)
		default:
			rw.WriteHeader(http.StatusBadRequest)
		}
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
