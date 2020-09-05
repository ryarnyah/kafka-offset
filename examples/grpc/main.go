package main

import (
	"github.com/hashicorp/go-plugin"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/plugin/shared"
)

type StdoutSink struct{}

func (StdoutSink) WriteKafkaMetrics(m []interface{}) error {
	return nil
}

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"kafka_grpc": &shared.KafkaGRPCPlugin{Impl: &StdoutSink{}},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
