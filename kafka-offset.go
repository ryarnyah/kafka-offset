package main

import (
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
)

var (
	sinkName = flag.String("sink", "log", "Sink to use (log, kafka, elasticsearch, collectd)")
	profile  = flag.String("profile", "prod", "Profile to apply to log")
	logLevel = flag.String("log-level", "info", "Log level")
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
	flag.Parse()

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
