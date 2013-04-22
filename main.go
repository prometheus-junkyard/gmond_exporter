package main

import (
	"flag"
	"github.com/prometheus/gmond_exporter/exporter"
	"log"
)

var configFile = flag.String("config", "gmond_exporter.conf", "config file.")

func main() {
	flag.Parse()

	exporter, err := exporter.New(*configFile)
	if err != nil {
		log.Fatalf("Couldn't instantiate exporter: %s", err)
	}

	exporter.Loop()
}
