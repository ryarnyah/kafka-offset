package shared

import (
	"context"
	"fmt"

	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/plugin/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GRPCClient is an implementation of KV that talks over RPC.
type GRPCClient struct{ client proto.KafkaPluginClient }

func (plugin *GRPCClient) WriteKafkaMetrics(m []interface{}) error {
	metricRequests := make([]*anypb.Any, 0)

	for _, metric := range m {
		switch metric := metric.(type) {
		case metrics.KafkaMeter:
			tags := make(map[string]string, len(metric.Meta))
			for k, v := range metric.Meta {
				tags[k] = fmt.Sprintf("%v", v)
			}
			r, err := anypb.New(&proto.KafkaMeter{
				Name:      metric.Name,
				Timestamp: timestamppb.New(metric.Timestamp),
				Meta:      tags,
				Count:     metric.Count(),
				Rate1:     metric.Rate1(),
				Rate5:     metric.Rate5(),
				Rate15:    metric.Rate15(),
				RateMean:  metric.RateMean(),
			})
			if err != nil {
				return err
			}
			metricRequests = append(metricRequests, r)
		case metrics.KafkaGauge:
			tags := make(map[string]string, len(metric.Meta))
			for k, v := range metric.Meta {
				tags[k] = fmt.Sprintf("%v", v)
			}
			r, err := anypb.New(&proto.KafkaGauge{
				Name:      metric.Name,
				Timestamp: timestamppb.New(metric.Timestamp),
				Meta:      tags,
				Value:     metric.Value(),
			})
			if err != nil {
				return err
			}
			metricRequests = append(metricRequests, r)
		}
	}

	_, err := plugin.client.WriteKafkaMetrics(context.Background(), &proto.WriteKafkaMetricsRequest{
		Metrics: metricRequests,
	})
	return err
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	// This is the real implementation
	Impl KafkaPlugin
}

func (s *GRPCServer) WriteKafkaMetrics(ctx context.Context, in *proto.WriteKafkaMetricsRequest) (*proto.Empty, error) {
	metricRequests := make([]interface{}, 0)

	for _, anymetric := range in.Metrics {
		metric, err := anymetric.UnmarshalNew()
		if err != nil {
			return &proto.Empty{}, err
		}
		metricRequests = append(metricRequests, metric)
	}

	err := s.Impl.WriteKafkaMetrics(metricRequests)
	return &proto.Empty{}, err
}
