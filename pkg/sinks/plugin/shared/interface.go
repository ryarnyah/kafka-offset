package shared

import (
	"context"

	"github.com/hashicorp/go-plugin"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/plugin/proto"
	"google.golang.org/grpc"
)

// Handshake is a common handshake that is shared by plugin and host.
var Handshake = plugin.HandshakeConfig{
	// This isn't required when using VersionedPlugins
	ProtocolVersion:  1,
	MagicCookieKey:   "KAFKA_OFFSET_PLUGIN",
	MagicCookieValue: "kafka-offset",
}

// PluginMap is the map of plugins we can dispense.
var PluginMap = map[string]plugin.Plugin{
	"kafka_grpc": &KafkaGRPCPlugin{},
}

// KafkaPlugin interface of sink plugin
type KafkaPlugin interface {
	WriteKafkaMetrics([]any) error
}

// KafkaGRPCPlugin go-plugin struct to make of plugin
type KafkaGRPCPlugin struct {
	// GRPCPlugin must still implement the Plugin interface
	plugin.Plugin
	// Concrete implementation, written in Go. This is only used for plugins
	// that are written in Go.
	Impl KafkaPlugin
}

// GRPCServer register kafka plugin over GRPC
func (p *KafkaGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	proto.RegisterKafkaPluginServer(s, &GRPCServer{Impl: p.Impl})
	return nil
}

// GRPCClient build GRPC client over go-plugin
func (p *KafkaGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (any, error) {
	return &GRPCClient{client: proto.NewKafkaPluginClient(c)}, nil
}
