package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/KimMachineGun/bloomfilterd/internal/store"

	"github.com/KimMachineGun/flago"
)

func main() {
	config := DefaultConfig

	err := flago.Bind(flag.CommandLine, &config)
	if err != nil {
		log.Fatalln(err)
	}
	flag.Parse()

	err = config.Validate()
	if err != nil {
		log.Fatalln(err)
	}

	s, err := store.New(&store.Config{
		N: config.N,
		P: config.P,

		NodeID:           config.NodeID,
		Addr:             config.Addr,
		TransportMaxPool: config.TransportMaxPool,
		TransportTimeout: config.TransportTimeout,
		Dir:              config.Dir,
		SnapshotRetain:   config.SnapshotRetain,

		ApplyTimeout: config.ApplyTimeout,
	})
	if err != nil {
		log.Fatalln(err)
	}

	if config.JoinAddr == "" {
		err = s.Bootstrap()
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		err = JoinCluster(config.NodeID, config.Addr, config.JoinAddr)
		if err != nil {
			log.Fatalln(err)
		}
	}

	err = http.ListenAndServe(config.HTTP, NewHandler(s))
	if err != nil {
		log.Fatalln(err)
	}
}
