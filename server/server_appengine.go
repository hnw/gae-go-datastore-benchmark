// How to deploy:
//   $ appcfg.py update . -A [application_id]

// +build appengine

package main

import (
	"fmt"
	"net/http"
	"sync"
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

type data struct {
	Name      string    `datastore:"name"`
	Value     string    `datastore:"value,noindex"`
	CreatedAt time.Time `datastore:"created_at"`
	UpdatedAt time.Time `datastore:"updated_at"`
}

func putSerialHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	for i := 0; i < 50; i++ {
		d := &data{
			Name:      fmt.Sprintf("name%d", i),
			Value:     fmt.Sprintf("value%d", i),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		k := datastore.NewKey(ctx, "data", fmt.Sprintf("name%d", i), 0, nil)
		k, err := datastore.Put(ctx, k, d)
		if err != nil {
			aelog.Infof(ctx, "%v", err)
		}
		aelog.Infof(ctx, "%v", k)
	}

	aelog.Infof(ctx, "datastore serial put finished\n")
	w.WriteHeader(200)
}

func putParallelHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			d := &data{
				Name:      fmt.Sprintf("name%d", i),
				Value:     fmt.Sprintf("value%d", i),
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			}
			k := datastore.NewKey(ctx, "data", fmt.Sprintf("name%d", i), 0, nil)
			k, err := datastore.Put(ctx, k, d)
			if err != nil {
				aelog.Infof(ctx, "%v", err)
			}
			aelog.Infof(ctx, "%v", k)
		}(i)
	}
	wg.Wait()

	aelog.Infof(ctx, "datastore parallel put finished\n")
	w.WriteHeader(200)
}

func putMultiHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	var ks []*datastore.Key
	var ds []*data

	for i := 0; i < 50; i++ {
		ks = append(ks, datastore.NewKey(ctx, "data", fmt.Sprintf("name%d", i), 0, nil))
		d := &data{
			Name:      fmt.Sprintf("name%d", i),
			Value:     fmt.Sprintf("value%d", i),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		ds = append(ds, d)
	}

	if _, err := datastore.PutMulti(ctx, ks, ds); err != nil {
		aelog.Infof(ctx, "failed to putmulti: %v", err)
	} else {
		for _, d := range ds {
			aelog.Infof(ctx, "d=%v", d)
		}
	}

	aelog.Infof(ctx, "datastore putmulti finished\n")
	w.WriteHeader(200)
}

func getSerialHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	for i := 0; i < 50; i++ {
		k := datastore.NewKey(ctx, "data", fmt.Sprintf("name%d", i), 0, nil)
		d := new(data)
		if err := datastore.Get(ctx, k, d); err != nil {
			aelog.Infof(ctx, "failed to get: %v", err)
		} else {
			aelog.Infof(ctx, "d=%v", d)
		}
	}

	aelog.Infof(ctx, "datastore serial get finished\n")
	w.WriteHeader(200)
}

func getSerialWithQueryHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	for i := 0; i < 50; i++ {
		q := datastore.NewQuery("data").
			Filter("name =", fmt.Sprintf("name%d", i))
			//Order("-created_at")

		var ds []data
		if _, err := q.GetAll(ctx, &ds); err != nil {
			aelog.Infof(ctx, "failed to get: %v", err)
		} else if len(ds) < 1 {
			aelog.Infof(ctx, "len(gs) == %d", len(ds))
		} else {
			aelog.Infof(ctx, "d=%v", ds[0])
		}
	}

	aelog.Infof(ctx, "datastore serial get with query finished\n")
	w.WriteHeader(200)
}

func getParallelHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := datastore.NewKey(ctx, "data", fmt.Sprintf("name%d", i), 0, nil)
			d := new(data)
			if err := datastore.Get(ctx, k, d); err != nil {
				aelog.Infof(ctx, "failed to get: %v", err)
			} else {
				aelog.Infof(ctx, "d=%v", d)
			}
		}(i)
	}
	wg.Wait()

	aelog.Infof(ctx, "datastore parallel get finished\n")
	w.WriteHeader(200)
}

func getMultiHandleFunc(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	var ks []*datastore.Key

	for i := 0; i < 50; i++ {
		ks = append(ks, datastore.NewKey(ctx, "data", fmt.Sprintf("name%d", i), 0, nil))
	}
	ds := make([]data, len(ks))
	if err := datastore.GetMulti(ctx, ks, ds); err != nil {
		aelog.Infof(ctx, "failed to get: %v", err)
	} else {
		for _, d := range ds {
			aelog.Infof(ctx, "d=%v", d)
		}
	}

	aelog.Infof(ctx, "datastore multi get finished\n")
	w.WriteHeader(200)
}
