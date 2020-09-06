package plugin

import (
	"crypto/tls"
	"os/exec"
	"strings"

	"github.com/hashicorp/go-plugin"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/plugin/shared"
	"github.com/ryarnyah/kafka-offset/pkg/util"
)

// NewPluginClient build new go-plugin client
func NewPluginClient(pluginCmd, pluginName string, tlsConfig *tls.Config, tlsEnabled bool) (*plugin.Client, plugin.ClientProtocol, shared.KafkaPlugin, error) {
	cmd := strings.Split(pluginCmd, " ")
	clientConfig := &plugin.ClientConfig{
		HandshakeConfig: shared.Handshake,
		Plugins:         shared.PluginMap,
		Cmd:             exec.Command(cmd[0], cmd[1:]...),
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolGRPC,
		},
		Logger: &util.HCLogAdapter{},
	}
	if tlsEnabled {
		clientConfig.TLSConfig = tlsConfig
	}
	client := plugin.NewClient(clientConfig)
	c, err := client.Client()
	if err != nil {
		return nil, nil, nil, err
	}
	s, err := c.Dispense(pluginName)
	if err != nil {
		defer c.Close()
		return nil, nil, nil, err
	}

	return client, c, s.(shared.KafkaPlugin), nil
}
