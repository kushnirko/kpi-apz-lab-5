package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
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

type responseBody struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type requestBody struct {
	Value any `json:"value"`
}

func handleGetRequest(rw http.ResponseWriter, r *http.Request, db *datastore.Db) {
	pathParts := strings.Split(r.URL.EscapedPath(), "/")
	if len(pathParts) != 3 {
		http.Error(rw, "invalid url path", http.StatusBadRequest)
		return
	}

	k := pathParts[2]
	t := r.URL.Query().Get("type")

	var v any
	var err error
	switch t {
	case "string", "":
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

	rw.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(rw).Encode(responseBody{Key: k, Value: v}); err != nil {
		http.Error(rw, "json encoding error", http.StatusInternalServerError)
	}
}

func handlePostRequest(rw http.ResponseWriter, r *http.Request, db *datastore.Db) {
	pathParts := strings.Split(r.URL.EscapedPath(), "/")
	if len(pathParts) != 3 {
		http.Error(rw, "invalid url path", http.StatusBadRequest)
		return
	}

	k := pathParts[2]

	var rb requestBody
	if err := json.NewDecoder(r.Body).Decode(&rb); err != nil {
		http.Error(rw, "json decoding error", http.StatusBadRequest)
		return
	}

	var err error
	switch v := rb.Value.(type) {
	case string:
		err = db.Put(k, v)
	case float64:
		if v == float64(int64(v)) {
			err = db.PutInt64(k, int64(v))
		} else {
			err = fmt.Errorf("non-integer value")
		}
	default:
		err = fmt.Errorf("unknown value type")
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
