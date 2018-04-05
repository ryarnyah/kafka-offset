# Kafka-Offset [![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fryarnyah%2Fkafka-offset.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fryarnyah%2Fkafka-offset?ref=badge_shield) [![Build Status](https://travis-ci.org/ryarnyah/kafka-offset.svg?branch=master)](https://travis-ci.org/ryarnyah/kafka-offset)

Kafka metrics offset fetcher with some sinks :D

## Installation

#### Binaries

- **linux** [amd64](https://github.com/ryarnyah/kafka-offset/releases/download/0.2.0/kafka-offset-linux-amd64)

#### Via Go

```bash
$ go get github.com/ryarnyah/kafka-offset
```

#### From Source

```bash
$ mkdir -p $GOPATH/src/github.com/ryarnyah
$ git clone https://github.com/ryarnyah/kafka-offset $GOPATH/src/github.com/ryarnyah/kafka-offset
$ cd !$
$ make
```

#### Running with Docker
```bash
docker run ryarnyah/kafka-offset:0.2.0 <option>
```

## Usage

```bash
Usage of ./kafka-offset:
  -elasticsearch-password string
    	Elasticsearch password
  -elasticsearch-sink-index string
    	Elasticsearch index name (default "metrics")
  -elasticsearch-sink-url string
    	Elasticsearch sink URL (default "http://localhost:9200")
  -elasticsearch-username string
    	Elasticsearch username
  -kafka-sink-brokers string
    	Kafka sink brokers (default "localhost:9092")
  -kafka-sink-sasl-password string
    	Kafka SASL password
  -kafka-sink-sasl-username string
    	Kafka SASL username
  -kafka-sink-ssl-cacerts string
    	Kafka SSL cacerts
  -kafka-sink-ssl-cert string
    	Kafka SSL cert
  -kafka-sink-ssl-insecure
    	Kafka insecure ssl connection
  -kafka-sink-ssl-key string
    	Kafka SSL key
  -kafka-sink-topic string
    	Kafka topic to send metrics (default "metrics")
  -kafka-sink-version string
    	Kafka sink broker version (default "0.10.2.0")
  -log-level string
    	Log level (default "info")
  -profile string
    	Profile to apply to log (default "prod")
  -sink string
    	Sink to use (log, kafka, elasticsearch) (default "log")
  -sink-produce-interval duration
    	Time beetween metrics production (default 1m0s)
  -source-brokers string
    	Kafka source brokers (default "localhost:9092")
  -source-kafka-version string
    	Kafka source broker version (default "0.10.2.0")
  -source-sasl-password string
    	Kafka SASL password
  -source-sasl-username string
    	Kafka SASL username
  -source-scrape-interval duration
    	Time beetween scrape kafka metrics (default 1m0s)
  -source-ssl-cacerts string
    	Kafka SSL cacerts
  -source-ssl-cert string
    	Kafka SSL cert
  -source-ssl-insecure
    	Kafka insecure ssl connection
  -source-ssl-key string
    	Kafka SSL key
```

### Supported metrics
| Name                                               | Exposed informations                                |
| -------------------------------------------------- | --------------------------------------------------- |
| `kafka_topic_partition`                            | Number of partitions for this Topic                 |
| `kafka_topic_partition_offset_newest`              | Newest offset for this Topic/Partition              |
| `kafka_topic_partition_offset_oldest`              | Oldest offset for this Topic/Partition              |
| `kafka_leader_topic_partition`                     | NodeID leader for this Topic/Partition              |
| `kafka_replicas_topic_partition`                   | Number of replicas for this Topic/Partition         |
| `kafka_in_sync_replicas`                           | Number of In-Sync Replicas for this Topic/Partition |
| `kafka_leader_is_preferred_topic_partition`        | 1 if Topic/Partition is using the Preferred Broker  |
| `kafka_under_replicated_topic_partition`           | 1 if Topic/Partition is under Replicated            |
| `kafka_consumer_group_lag`                         | Lag for this Consumer group / Topic                 |
| `kafka_consumer_group_latest_offset`               | Latest offset for this Consumer group               |
| `kafka_topic_rate`                                 | Rate of Topic production                            |
| `kafka_consumer_group_rate`                        | Rate of this Consumer group consumer on this Topic  |

## About

### Supported Sinks

#### Log (-sink log)
Simple log sink with logrus

##### Example
```bash
docker run ryarnyah/kafka-offset:0.2.0 -sink log -source-brokers localhost:9092
```

#### Kafka (-sink kafka)
Kafka sink export metrics as JSON format to specified topic. SASL/SSL supported.

##### Example
```bash
docker run ryarnyah/kafka-offset:0.2.0 -sink kafka -source-brokers localhost:9092 -kafka-sink-brokers localhost:9092 -kafka-sink-topic metrics
```

#### Elasticsearch (-sink elasticsearch)
Elasticsearch V6 sink export metrics as documents to specified index. Auth supported.

##### Example
```bash
docker run ryarnyah/kafka-offset:0.2.0 -sink elasticsearch -source-brokers localhost:9092 -elasticsearch-sink-url localhost:9200 -elasticsearch-sink-index metrics
```
