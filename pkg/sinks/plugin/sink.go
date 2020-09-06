package plugin

import (
	"flag"

	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/plugin/shared"
	"github.com/ryarnyah/kafka-offset/pkg/util"
)

// Sink default sink use logrus to print metrics
type Sink struct {
	*common.Sink

	plugin shared.KafkaPlugin
}

var (
	pluginCmd            = flag.String("plugin-cmd", "", "Command to launch the plugin with arguments (ex: /usr/local/bin/my-plugin --test)")
	pluginTLSCaFile      = flag.String("plugin-tls-ca-file", "", "TLS CA file (client trust)")
	pluginTLSCertFile    = flag.String("plugin-tls-cert-file", "", "TLS certificate file (client trust)")
	pluginTLSCertKeyFile = flag.String("plugin-tls-cert-key-file", "", "TLS certificate key file (client trust)")
	pluginTLSInsecure    = flag.Bool("plugin-tls-insecure", false, "Check TLS certificate against CA File")
	pluginName           = flag.String("plugin-name", "kafka_grpc", "Plugin type to use. Only kafka_grpc is supported by now.")
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

	tlsConfig, tlsEnabled, err := util.GetTLSConfiguration(*pluginTLSCaFile, *pluginTLSCertFile, *pluginTLSCertKeyFile, *pluginTLSInsecure)
	if err != nil {
		return nil, err
	}
	client, _, s, err := NewPluginClient(*pluginCmd, *pluginName, tlsConfig, tlsEnabled)
	if err != nil {
		return nil, err
	}

	sink.plugin = s
	sink.KafkaMetricsFunc = sink.kafkaMetrics
	sink.CloseFunc = func() error {
		client.Kill()
		return nil
	}

	sink.Run()

	return sink, nil
}
