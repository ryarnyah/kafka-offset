package main

import (
	_ "github.com/ryarnyah/kafka-offset/pkg/sinks/collectd"
	_ "github.com/ryarnyah/kafka-offset/pkg/sinks/elasticsearch"
	_ "github.com/ryarnyah/kafka-offset/pkg/sinks/influxdb"
	_ "github.com/ryarnyah/kafka-offset/pkg/sinks/kafka"
	_ "github.com/ryarnyah/kafka-offset/pkg/sinks/log"
)
