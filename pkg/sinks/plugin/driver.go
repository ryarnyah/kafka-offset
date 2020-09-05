package plugin

import (
	"flag"
	"os/exec"
	"strings"

	"github.com/hashicorp/go-plugin"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/plugin/shared"
)

// Sink default sink use logrus to print metrics
type Sink struct {
	*common.Sink

	plugin shared.KafkaPlugin
}

var (
	pluginCmd  = flag.String("plugin-cmd", "", "Command to launch the plugin with arguments (ex: /usr/local/bin/my-plugin --test)")
	pluginName = flag.String("plugin-name", "kafka_grpc", "Plugin type to use. Only kafka_grpc is supported by now.")
)

func init() {
	metrics.RegisterSink("plugin", NewSink)
}

func (s *Sink) kafkaMetrics(m []interface{}) error {
	return s.plugin.WriteKafkaMetrics(m)
}

// NewSink build sink
func NewSink() (metrics.Sink, error) {
	sink := &Sink{
		Sink: common.NewCommonSink(),
	}

	cmd := strings.Split(*pluginCmd, " ")
	client := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: shared.Handshake,
		Plugins:         shared.PluginMap,
		Cmd:             exec.Command(cmd[0], cmd[1:]...),
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolGRPC,
		},
	})
	c, err := client.Client()
	if err != nil {
		return nil, err
	}
	s, err := c.Dispense(*pluginName)
	if err != nil {
		return nil, err
	}

	sink.plugin = s.(shared.KafkaPlugin)
	sink.KafkaMetricsFunc = sink.kafkaMetrics
	sink.CloseFunc = c.Close

	sink.Run()

	return sink, nil
}
