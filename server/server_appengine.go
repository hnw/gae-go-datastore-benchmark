// How to deploy:
//   $ appcfg.py update . -A [application_id]

// +build appengine

package main

import (
	"fmt"
	"net/http"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	aelog "google.golang.org/appengine/log"
)

func init() {
	http.HandleFunc("/putSerial", putSerialHandleFunc)
	http.HandleFunc("/putParallel", putParallelHandleFunc)
	http.HandleFunc("/putMulti", putMultiHandleFunc)

	http.HandleFunc("/getSerial", getSerialHandleFunc)
	http.HandleFunc("/getSerialWithQuery", getSerialWithQueryHandleFunc)
	http.HandleFunc("/getParallel", getParallelHandleFunc)
	http.HandleFunc("/getMulti", getMultiHandleFunc)
}

type Data struct {
	Name      string    `datastore:"name"`
	Value     string    `datastore:"value,noindex"`
	CreatedAt time.Time `datastore:"created_at"`
	UpdatedAt time.Time `datastore:"updated_at"`
}

func putSerialHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	for i := 0; i < 50; i++ {
		d := &Data{
			Name:      fmt.Sprintf("name%d", i),
			Value:     fmt.Sprintf("value%d", i),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		k := datastore.NewKey(ctx, "Data", fmt.Sprintf("name%d", i), 0, nil)
		k, err := datastore.Put(ctx, k, d)
		if err != nil {
			aelog.Infof(ctx, "%v", err)
		}
		aelog.Infof(ctx, "%v", k)
	}

	aelog.Infof(ctx, "datastore put finished\n")
	w.WriteHeader(200)
}

func putParallelHandleFunc(w http.ResponseWriter, r *http.Request) {

}

func putMultiHandleFunc(w http.ResponseWriter, r *http.Request) {

}

func getSerialHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	for i := 0; i < 50; i++ {
		k := datastore.NewKey(ctx, "Data", fmt.Sprintf("name%d", i), 0, nil)
		d := new(Data)
		if err := datastore.Get(ctx, k, d); err != nil {
			aelog.Infof(ctx, "failed to get: %v", err)
		} else {
			aelog.Infof(ctx, "d=%v", d)
		}
	}

	aelog.Infof(ctx, "datastore get finished\n")
	w.WriteHeader(200)
}

func getSerialWithQueryHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	for i := 0; i < 50; i++ {
		q := datastore.NewQuery("Data").
			Filter("name =", fmt.Sprintf("name%d", i))
			//Order("-created_at")

		var ds []Data
		if _, err := q.GetAll(ctx, &ds); err != nil {
			aelog.Infof(ctx, "failed to get: %v", err)
		} else if len(ds) < 1 {
			aelog.Infof(ctx, "len(gs) == %d", len(ds))
		} else {
			aelog.Infof(ctx, "d=%v", ds[0])
		}
	}

	aelog.Infof(ctx, "datastore get finished\n")
	w.WriteHeader(200)
}

func getParallelHandleFunc(w http.ResponseWriter, r *http.Request) {

}

func getMultiHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	var ks []*datastore.Key

	for i := 0; i < 50; i++ {
		ks = append(ks, datastore.NewKey(ctx, "Data", fmt.Sprintf("name%d", i), 0, nil))
	}
	ds := make([]Data, len(ks))
	if err := datastore.GetMulti(ctx, ks, ds); err != nil {
		aelog.Infof(ctx, "failed to get: %v", err)
	} else {
		for _, d := range ds {
			aelog.Infof(ctx, "d=%v", d)
		}
	}

	aelog.Infof(ctx, "datastore get finished\n")
	w.WriteHeader(200)
}
