package plugin

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/go-plugin"
	"github.com/rcrowley/go-metrics"
	common_metrics "github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/plugin/shared"
)

func TestNewPluginClient(t *testing.T) {
	cmd := fmt.Sprintf("%s -test.run=TestHelperProcess -- mock", os.Args[0])
	client, _, _, err := NewPluginClient(cmd, "kafka_grpc", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Kill()
}

func TestAddMetrics(t *testing.T) {
	cmd := fmt.Sprintf("%s -test.run=TestHelperProcess -- mock", os.Args[0])
	client, _, p, err := NewPluginClient(cmd, "kafka_grpc", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Kill()

	testMetrics := make([]any, 0)
	testMetrics = append(testMetrics, common_metrics.KafkaMeter{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]any),
		},
		Meter: metrics.NilMeter{},
	})
	testMetrics = append(testMetrics, common_metrics.KafkaGauge{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]any),
		},
		Gauge: metrics.NilGauge{},
	})

	err = p.WriteKafkaMetrics(testMetrics)

	if err != nil {
		t.Fatal(err)
	}
}

func TestHelperProcess(t *testing.T) {
	args := os.Args
	for len(args) > 0 {
		if args[0] == "--" {
			args = args[1:]
			break
		}

		args = args[1:]
	}

	if len(args) == 0 {
		return
	}

	defer os.Exit(0)
	cmd, _ := args[0], args[1:]
	switch cmd {
	case "mock":
		plugin.Serve(&plugin.ServeConfig{
			HandshakeConfig: shared.Handshake,
			Plugins: map[string]plugin.Plugin{
				"kafka_grpc": &shared.KafkaGRPCPlugin{Impl: &testSink{}},
			},
			GRPCServer: plugin.DefaultGRPCServer,
		})
		// Shouldn't reach here but make sure we exit anyways
		os.Exit(0)
	}
}

type testSink struct {
	savedMetrics []any
}

func (s *testSink) WriteKafkaMetrics(m []any) error {
	s.savedMetrics = append(s.savedMetrics, m...)
	return nil
}
