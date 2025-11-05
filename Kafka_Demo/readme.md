# Kafka Demo with Docker Compose

This project demonstrates a complete Apache Kafka setup using Docker Compose with Python producers and consumers that showcase key Kafka concepts and (not too fancy) patterns. 

This readme.md is created with the help of gpt4o and Claude
## Overview

This project provides:

- A complete Kafka environment with ZooKeeper and web UI
- Python examples demonstrating producer and consumer patterns
- Code samples showing partitioning, consumer groups, and offset management

## Architecture


Components:
- **ZooKeeper**: Handles cluster coordination
- **Kafka Broker**: Processes and stores messages
- **Schema Registry**: Manages Avro schemas (undergoing)
- **Kafka UI**: Web interface for monitoring
- **Python Demo Container**: Runs producer/consumer examples

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.8+ (for local development)

### Running the Environment



1. Start the Kafka environment:
   ```bash
   docker-compose up -d
   ```

2. Verify the services are running:
   ```bash
   docker-compose ps
   ```

3. Access the Kafka UI:
   - Open [http://localhost:8080](http://localhost:8080) in your browser

### Running the Demos

Connect to the Python container:

```bash
docker exec -it python-kafka-demo bash
```

Run the full demo:
```bash
python run_demos.py --type both
```

Run specific demos:
```bash
# Producer partitioning strategies
python run_demos.py --demo producer:partitioning

# Producer delivery semantics
python run_demos.py --demo producer:delivery

# Consumer offset management
python run_demos.py --demo consumer:offset

# Consumer group behavior
python run_demos.py --demo consumer:group
```

## Key Concepts Demonstrated

### Producer Patterns

1. **Partitioning Strategies**
   - Random partitioning (no key)
   - Key-based partitioning (consistent hashing)
   - Explicit (manual) partition selection

2. **Delivery Guarantees**
   - At-most-once: Fire and forget (acks=0)
   - At-least-once: With retries (acks=all)
   - Exactly-once: Idempotent producers (undergoing)

3. **Performance Optimization**
   - Message batching
   - Compression (gzip, snappy)
   - Asynchronous production


### Consumer Patterns

1. **Consumer Groups**
   - Partition assignment
   - Load balancing
   - Rebalancing

2. **Offset Management**
   - Automatic commits
   - Manual commits
   - Commit strategies

3. **Consumption Patterns**
   - Batch processing
   - Stream processing
   - Seeking to positions

## Code Structure

```
.
├── docker-compose.yml              # Docker environment setup
├── python-kafka/
│   ├── Dockerfile                  # Python environment
│   ├── requirements.txt            # Python dependencies
│   ├── producer.py                 # Producer demonstration
│   ├── consumer.py                 # Consumer demonstration
│   └── run_demos.py                # Demo runner script
└── README.md                       # This file
```

## Docker Compose Services

| Service          | Port(s)            | Description                       |
|------------------|-------------------|-----------------------------------|
| zookeeper        | 2181              | Cluster coordinator               |
| kafka            | 9092, 29092       | broker/kafka server               |
| schema-registry  | 8081              | Schema management                 |
| kafka-ui         | 8080              | Web UI for monitoring             |
| python-kafka-demo| -                 | Python demo environment           |

## Kafka Topics

The environment sets up the following topics:

- **orders** (3 partitions): For order-related messages
- **notifications** (2 partitions): For notification events
- **logs** (1 partition): For logging events
- **inventory** and **shipments**: Used by the transactional demo

## Advanced Usage

### Scaling Consumers

To see consumer group rebalancing in action:

```bash
# Start multiple consumer instances
docker exec -it python-kafka-demo python consumer.py --demo group --group test-group-1
docker exec -it python-kafka-demo python consumer.py --demo group --group test-group-1
```

### Custom Topic Configuration

Modify the `kafka-setup` service in `docker-compose.yml` to create topics with different configurations.

### Message Browsing

Use the Kafka UI to browse messages in topics and monitor consumer groups.
Also connect to Offset Explorer if you'd like, to see how msgs are handled

## Troubleshooting

### Common Issues

- **Connection refused**: Ensure Kafka broker is healthy (`docker-compose logs kafka`)
- **Consumer not receiving messages**: Check consumer group offsets in UI
- **Producer errors**: Verify topic exists and has correct partitions

### Resetting the Environment

To completely reset:

```bash
docker-compose down -v
docker-compose up -d
```

## Resources (thanks to Claude)

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Python Client](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [Kafka: The Definitive Guide](https://www.oreilly.com/library/view/kafka-the-definitive/9781491936153/)

## License

MIT License
