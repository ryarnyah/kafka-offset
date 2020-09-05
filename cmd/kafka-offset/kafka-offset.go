package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/version"
	"github.com/sirupsen/logrus"
)

var (
	sinkName = flag.String("sink", "log", "Sink to use (log, kafka, elasticsearch, collectd, plugin)")
	profile  = flag.String("profile", "prod", "Profile to apply to log")
	logLevel = flag.String("log-level", "info", "Log level")

	profiling     = flag.Bool("profiling-enable", false, "Enable profiling")
	profilingHost = flag.String("profiling-host", "localhost:6060", "HTTP profiling host:port")
	v             = flag.Bool("version", false, "Print version")
)

const (
	// BANNER for usage.
	BANNER = `
 _  __      __ _                ___   __  __          _
| |/ /__ _ / _| | ____ _       / _ \ / _|/ _|___  ___| |_
| ' // _` + "`" + ` | |_| |/ / _` + "`" + ` |_____| | | | |_| |_/ __|/ _ \ __|
| . \ (_| |  _|   < (_| |_____| |_| |  _|  _\__ \  __/ |_
|_|\_\__,_|_| |_|\_\__,_|      \___/|_| |_| |___/\___|\__|

 Scrape kafka metrics and send them to some sinks!
 Version: %s
 Build: %s

`
)

func installSignalHandler(stopChs ...chan interface{}) *sync.WaitGroup {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	var wg sync.WaitGroup

	wg.Add(1)
	// Block until a signal is received.
	go func() {
		sig := <-c
		for _, stopCh := range stopChs {
			close(stopCh)
		}
		logrus.Infof("Exiting given signal: %v", sig)
		wg.Done()
	}()
	return &wg
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, BANNER, version.VERSION, version.GITCOMMIT)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *v {
		fmt.Printf(BANNER, version.VERSION, version.GITCOMMIT)
		return
	}

	if *profiling {
		go func() {
			logrus.Error(http.ListenAndServe(*profilingHost, http.DefaultServeMux))
		}()
	}

	if *profile == "prod" {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}

	level, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logrus.Error(err)
		return
	}
	logrus.SetLevel(level)

	sink, err := metrics.New(*sinkName)
	if err != nil {
		logrus.Error(err)
		return
	}
	defer func() {
		err := sink.Close()
		if err != nil {
			logrus.Error(err)
		}
	}()
	s, err := metrics.NewKafkaSource(sink)
	if err != nil {
		logrus.Error(err)
		return
	}
	defer func() {
		err := s.Close()
		if err != nil {
			logrus.Error(err)
		}
	}()

	// Wait until signal
	stopCh := s.Run()
	wg := installSignalHandler(stopCh)
	<-stopCh

	// Wait until cleanup
	wg.Wait()
	s.Wait()
}
