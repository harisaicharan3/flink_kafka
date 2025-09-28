# Flink-Kafka Learning Project

A comprehensive learning project that demonstrates all basic operations of Apache Kafka and Apache Flink through a real-time e-commerce transaction processing system.

## 🎯 Learning Objectives

This project covers:

### Kafka Operations
- **Producer**: Creating and sending messages to topics
- **Consumer**: Reading and processing messages from topics
- **Topics**: Creating and managing Kafka topics
- **Partitioning**: Understanding message partitioning and keys
- **Consumer Groups**: Managing multiple consumers
- **Serialization**: JSON message serialization/deserialization

### Flink Operations
- **Stream Processing**: Real-time data processing
- **Map Functions**: Data transformation
- **Filter Functions**: Data filtering
- **Keyed Operations**: Grouping and aggregating by keys
- **Window Operations**: Time-based aggregations
- **Sink Operations**: Writing processed data back to Kafka
- **Watermarks**: Handling event time and late data

## 🏗️ Architecture

```
Kafka Producer → transactions topic → Flink Processor → processed_transactions topic
                                                      → user_aggregations topic
                                                      → Console Output
```

## 📋 Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Basic understanding of streaming data concepts

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Clone or download the project
cd flink_kafka

# Run the setup script
./setup.sh
```

The setup script will:
- Create a Python virtual environment
- Install required dependencies
- Start Kafka and Flink services via Docker
- Create necessary Kafka topics

### 2. Activate Virtual Environment

```bash
source venv/bin/activate
```

### 3. Run the Demo

Open multiple terminals and run these commands in order:

**Terminal 1 - Start Flink Processing Job:**
```bash
python flink_processor.py
```

**Terminal 2 - Start Data Producer:**
```bash
python kafka_producer.py
```

**Terminal 3 - Monitor Results:**
```bash
python kafka_consumer.py
```

## 📊 What You'll See

### Data Flow
1. **Producer** generates sample e-commerce transaction data
2. **Flink** processes the data with various operations:
   - Filters high-value transactions (> $100)
   - Applies category-based discounts
   - Aggregates by user and category
   - Creates time-windowed summaries
3. **Consumer** displays the processed results

### Sample Data Structure
```json
{
  "transaction_id": "txn_1234567890_1234",
  "user_id": "user_123",
  "product": "laptop",
  "category": "electronics",
  "amount": 999.99,
  "quantity": 1,
  "payment_method": "credit_card",
  "timestamp": "2024-01-15T10:30:00",
  "location": "city_5"
}
```

## 🔧 Components Explained

### 1. Kafka Producer (`kafka_producer.py`)
- Generates realistic e-commerce transaction data
- Demonstrates message keying for partitioning
- Shows batch processing and error handling
- Produces to multiple topics

**Key Concepts:**
- Message serialization
- Producer configuration (acks, retries, batching)
- Topic partitioning strategies

### 2. Flink Processor (`flink_processor.py`)
- Consumes from Kafka topics
- Implements various Flink operations:
  - **Map**: Data transformation and enrichment
  - **Filter**: High-value transaction filtering
  - **KeyBy**: User-based grouping
  - **Window**: Time-based aggregations
  - **Reduce**: Category-wise sales totals
- Writes results back to Kafka topics

**Key Concepts:**
- Stream processing patterns
- Event time vs processing time
- Window operations
- State management

### 3. Kafka Consumer (`kafka_consumer.py`)
- Consumes from multiple topics
- Demonstrates different consumption patterns
- Shows real-time monitoring capabilities

**Key Concepts:**
- Consumer groups
- Offset management
- Message deserialization
- Error handling

## 🎓 Learning Exercises

### Beginner
1. **Modify the producer** to generate different types of data
2. **Change filter criteria** in the Flink job
3. **Add new fields** to the transaction data structure

### Intermediate
1. **Implement custom serializers** for complex data types
2. **Add error handling** and dead letter queues
3. **Create custom window functions** for different aggregation logic

### Advanced
1. **Add exactly-once processing** semantics
2. **Implement complex event processing** (CEP) patterns
3. **Add monitoring and metrics** collection
4. **Scale the system** with multiple partitions and parallelism

## 🛠️ Configuration

### Kafka Topics
- `transactions`: Raw transaction data
- `processed_transactions`: Enriched and filtered data
- `user_aggregations`: User-wise spending summaries
- `high_value_transactions`: Transactions > $500

### Flink Configuration
- Parallelism: 1 (for demo purposes)
- Checkpointing: Disabled (can be enabled for production)
- Watermarks: Processing time based

## 📈 Monitoring

### Kafka UI
Access at: http://localhost:8080
- View topics and partitions
- Monitor consumer groups
- Inspect message contents

### Flink Dashboard
Access at: http://localhost:8081
- Monitor job execution
- View metrics and throughput
- Check task manager status

## 🐛 Troubleshooting

### Common Issues

1. **Services not starting**
   ```bash
   docker-compose logs kafka
   docker-compose logs flink
   ```

2. **Python import errors**
   ```bash
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Kafka connection errors**
   - Ensure Docker services are running
   - Check if ports 9092, 8080, 8081 are available

4. **Flink job failures**
   - Check Flink dashboard for error details
   - Verify Kafka connector JARs are available

### Useful Commands

```bash
# Check running containers
docker ps

# View Kafka logs
docker logs kafka

# List Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Stop all services
docker-compose down

# Restart services
docker-compose restart
```

## 📚 Further Learning

### Kafka Resources
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Streams](https://kafka.apache.org/documentation/streams/)
- [Confluent Kafka Tutorials](https://developer.confluent.io/tutorials/)

### Flink Resources
- [Flink Documentation](https://flink.apache.org/docs/)
- [Flink Training](https://training.ververica.com/)
- [Flink Examples](https://github.com/apache/flink/tree/master/flink-examples)

### Advanced Topics
- Schema Registry and Avro
- Exactly-once processing
- Complex Event Processing (CEP)
- Machine Learning with Flink
- Kubernetes deployment

## 🤝 Contributing

Feel free to extend this project with:
- Additional data sources
- More complex processing logic
- Better error handling
- Performance optimizations
- Additional monitoring

## 📄 License

This project is for educational purposes. Feel free to use and modify as needed.

---

**Happy Learning! 🚀**

Start with the setup script and explore each component to understand how Kafka and Flink work together for real-time data processing.
