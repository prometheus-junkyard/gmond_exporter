package main

import (
	"bufio"
	"encoding/xml"
	"flag"
	"github.com/prometheus/client_golang"
	"github.com/prometheus/client_golang/metrics"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

const (
	proto = "tcp"
)

var (
  listeningAddress      = flag.String("listeningAddress", ":8080", "Address on which to expose JSON metrics.")
  metricsEndpoint       = flag.String("metricsEndpoint", "/metrics.json", "Path under which to expose JSON metrics.")
	gangliaAddress        = flag.String("gangliaAddress", "ganglia:8649", "gmond address.")
	gangliaScrapeInterval = flag.Int("gangliaScrapeInterval", 60, "Interval in seconds between scrapes.")
)

type ExtraElement struct {
	Name string `xml:"NAME,attr"`
	Val  string `xml:"VAL,attr"`
}

type ExtraData struct {
	ExtraElements []ExtraElement `xml:"EXTRA_ELEMENT"`
}

type Metric struct {
	Name      string    `xml:"NAME,attr"`
	Value     float64   `xml:"VAL,attr"`
	Unit      string    `xml:"UNITS,attr"`
	Slope     string    `xml:"SLOPE,attr"`
	Tn        int       `xml:"TN,attr"`
	Tmax      int       `xml:"TMAX,attr"`
	Dmax      int       `xml:"DMAX,attr"`
	ExtraData ExtraData `xml:"EXTRA_DATA"`
}

type Host struct {
	Name         string `xml:"NAME,attr"`
	Ip           string `xml:"IP,attr"`
	Tags         string `xml:"TAGS,attr"`
	Reported     int    `xml:"REPORTED,attr"`
	Tn           int    `xml:"TN,attr"`
	Tmax         int    `xml:"TMAX,attr"`
	Dmax         int    `xml:"DMAX,attr"`
	Location     string `xml:"LOCATION,attr"`
	GmondStarted int    `xml:"GMOND_STARTED",attr"`

	Metrics []Metric `xml:"METRIC"`
}

type Cluster struct {
	Name      string `xml:"NAME,attr"`
	Owner     string `xml:"OWNER,attr"`
	LatLong   string `xml:"LATLONG,attr"`
	Url       string `xml:"URL,attr"`
	Localtime int    `xml:"LOCALTIME,attr"`
	Hosts     []Host `xml:"HOST"`
}

type Ganglia struct {
	XMLNAME  xml.Name  `xml:"GANGLIA_XML"`
	Clusters []Cluster `xml:"CLUSTER"`
}

var gaugePerGangliaMetrics map[string]metrics.Gauge

func init() {
	gaugePerGangliaMetrics = make(map[string]metrics.Gauge)
}

func serveStatus() {
	exporter := registry.DefaultRegistry.Handler()

  http.Handle(*metricsEndpoint, exporter)
  http.ListenAndServe(*listeningAddress, nil)
}

func toUtf8(charset string, input io.Reader) (io.Reader, error) {
	return input, nil
}

func fetchMetrics(gmond_reader io.Reader) {
	ganglia := Ganglia{}
	decoder := xml.NewDecoder(gmond_reader)
	decoder.CharsetReader = toUtf8

	err := decoder.Decode(&ganglia)
	if err != nil {
		log.Fatalf("Error: Couldn't parse xml: %s", err)
	}

	var metricsUsed []string
	for _, cluster := range ganglia.Clusters {
		for _, host := range cluster.Hosts {

			for _, metric := range host.Metrics {
				if _, ok := gaugePerGangliaMetrics[metric.Name]; !ok {
					metricsUsed = append(metricsUsed, metric.Name) // keep track metrics we actually update
					var desc string
					var title string
					for _, element := range metric.ExtraData.ExtraElements {
						switch element.Name {
						case "DESC":
							desc = element.Val
						case "TITLE":
							title = element.Val
						}
						if title != "" && desc != "" {
							break
						}
					}
					gauge := metrics.NewGauge()
					gaugePerGangliaMetrics[metric.Name] = gauge
					registry.DefaultRegistry.Register(metric.Name, desc, registry.NilLabels, gauge) // one gauge per metric!
				}

				var labels = map[string]string{
					"hostname": host.Name,
					"cluster":  cluster.Name,
				}
				gaugePerGangliaMetrics[metric.Name].Set(labels, metric.Value)
			}
		}
	}

	for metricGlobal, _ := range gaugePerGangliaMetrics {
		if !isIn(metricGlobal, metricsUsed) {
			log.Printf("%s not in use anymore, deleting", metricGlobal)
			delete(gaugePerGangliaMetrics, metricGlobal)
		}
	}
}

func isIn(element string, list []string) bool {
	for _, e := range list {
		if e == element {
			return true
		}
	}
	return false
}

func main() {
	flag.Parse()
	go serveStatus()
	func() {
		for {
			log.Printf("Scraping %s", *gangliaAddress)
			conn, err := net.Dial(proto, *gangliaAddress)
			if err != nil {
				log.Fatalf("Can't connect to gmond: %s", err)
			}
			fetchMetrics(bufio.NewReader(conn))
			time.Sleep(time.Duration(*gangliaScrapeInterval) * time.Second)
		}
	}()
}
