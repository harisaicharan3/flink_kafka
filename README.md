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
- **Binary Data**: Handling video frames and binary content

### Flink Operations
- **Stream Processing**: Real-time data processing
- **Map Functions**: Data transformation
- **Filter Functions**: Data filtering
- **Keyed Operations**: Grouping and aggregating by keys
- **Window Operations**: Time-based aggregations
- **Sink Operations**: Writing processed data back to Kafka
- **Watermarks**: Handling event time and late data
- **Video Processing**: Real-time video frame analysis

## 🏗️ Architecture

### Transaction Processing
```
Kafka Producer → transactions topic → Flink Processor → processed_transactions topic
                                                      → user_aggregations topic
                                                      → Console Output
```

### Video Processing
```
Video Producer → video_frames topic → Flink Video Processor → processed_video_frames topic
                                                           → video_source_aggregations topic
                                                           → Console Output
```

### File Upload Processing
```
Web UI → uploaded_documents/videos/images topics → Flink File Processor → processed_files topic
                                                                        → file_type_aggregations topic
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

### 3. Run the Demos

#### Transaction Processing Demo
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

#### Video Processing Demo
**Terminal 1 - Start Video Flink Processing Job:**
```bash
python video_flink_processor.py
```

**Terminal 2 - Start Video Producer:**
```bash
python video_frame_producer.py
```

**Terminal 3 - Monitor Video Results:**
```bash
python video_frame_consumer.py
```

#### File Upload Processing Demo
**Terminal 1 - Start File Flink Processing Job:**
```bash
python file_flink_processor.py
```

**Terminal 2 - Start Web UI:**
```bash
python web_ui.py
```

**Terminal 3 - Monitor File Results:**
```bash
python file_upload_consumer.py
```

#### Complete Demo Runner
```bash
# Run complete transaction demo
python demo.py

# Run complete video demo
python video_demo.py

# Access web UI for file uploads
# Open browser: http://localhost:5000
```

## 📊 What You'll See

### Transaction Data Flow
1. **Producer** generates sample e-commerce transaction data
2. **Flink** processes the data with various operations:
   - Filters high-value transactions (> $100)
   - Applies category-based discounts
   - Aggregates by user and category
   - Creates time-windowed summaries
3. **Consumer** displays the processed results

### Video Data Flow
1. **Video Producer** captures/generates video frames
2. **Flink** processes video frames with various operations:
   - Filters high-resolution frames (> 480p)
   - Analyzes frame properties (brightness, aspect ratio, compression)
   - Aggregates by source and resolution category
   - Creates time-windowed video statistics
3. **Video Consumer** displays processed frame analysis

### File Upload Data Flow
1. **Web UI** allows users to upload documents, videos, and images
2. **Flink** processes uploaded files with various operations:
   - Filters large files (> 1MB)
   - Extracts metadata (text content, video properties, image dimensions)
   - Analyzes file properties (size categories, quality metrics)
   - Aggregates by file type and size category
   - Creates time-windowed file statistics
3. **File Consumer** displays processed file analysis and can save files locally

### Sample Data Structures

#### Transaction Data
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

#### Video Frame Data
```json
{
  "frame_id": "frame_123_1705123456789",
  "timestamp": "2024-01-15T10:30:00",
  "source": "webcam",
  "frame_data": "base64_encoded_jpeg_data...",
  "frame_width": 640,
  "frame_height": 480,
  "frame_channels": 3,
  "frame_size_bytes": 45678,
  "metadata": {}
}
```

#### Uploaded File Data
```json
{
  "file_id": "document_1705123456789",
  "filename": "report.pdf",
  "file_type": "document",
  "file_size": 2048576,
  "file_hash": "md5_hash_here",
  "upload_timestamp": "2024-01-15T10:30:00",
  "file_content": "base64_encoded_file_content...",
  "metadata": {
    "text_content": "Document text content...",
    "mime_type": "application/pdf"
  }
}
```

## 🔧 Components Explained

### Transaction Processing Components

#### 1. Kafka Producer (`kafka_producer.py`)
- Generates realistic e-commerce transaction data
- Demonstrates message keying for partitioning
- Shows batch processing and error handling
- Produces to multiple topics

**Key Concepts:**
- Message serialization
- Producer configuration (acks, retries, batching)
- Topic partitioning strategies

#### 2. Flink Processor (`flink_processor.py`)
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

#### 3. Kafka Consumer (`kafka_consumer.py`)
- Consumes from multiple topics
- Demonstrates different consumption patterns
- Shows real-time monitoring capabilities

**Key Concepts:**
- Consumer groups
- Offset management
- Message deserialization
- Error handling

### Video Processing Components

#### 4. Video Frame Producer (`video_frame_producer.py`)
- Captures frames from webcam, video files, or generates sample frames
- Converts frames to base64 for Kafka transmission
- Demonstrates binary data handling with Kafka
- Supports multiple video sources

**Key Concepts:**
- Binary data serialization
- Video frame encoding/decoding
- Real-time video capture
- Base64 encoding for JSON transport

#### 5. Video Flink Processor (`video_flink_processor.py`)
- Processes video frames in real-time
- Implements video-specific operations:
  - **Filter**: High-resolution frame filtering
  - **Map**: Frame analysis and metadata extraction
  - **KeyBy**: Source-based grouping
  - **Window**: Time-based video statistics
- Analyzes frame properties without full OpenCV dependency

**Key Concepts:**
- Binary data processing in Flink
- Video frame analysis
- Metadata extraction
- Real-time video statistics

#### 6. Video Frame Consumer (`video_frame_consumer.py`)
- Consumes and displays video frames
- Performs frame analysis and visualization
- Supports multiple consumption modes (display, analyze, save)
- Demonstrates video frame reconstruction

**Key Concepts:**
- Video frame reconstruction
- Real-time video display
- Frame analysis and processing
- Multiple consumption patterns

### File Upload Processing Components

#### 7. Web UI (`web_ui.py`)
- Provides a modern web interface for file uploads
- Supports drag-and-drop file uploads
- Real-time connection status monitoring
- File type validation and metadata extraction
- Responsive design with progress indicators

**Key Concepts:**
- Web-based file upload interface
- Real-time Kafka connection monitoring
- File type detection and validation
- Base64 encoding for binary data transport

#### 8. File Flink Processor (`file_flink_processor.py`)
- Processes uploaded files in real-time
- Implements file-specific operations:
  - **Filter**: Large file filtering (> 1MB)
  - **Map**: File analysis and metadata extraction
  - **KeyBy**: File type-based grouping
  - **Window**: Time-based file statistics
- Analyzes documents, videos, and images

**Key Concepts:**
- Multi-topic file processing
- File metadata extraction
- Size-based categorization
- File type aggregations

#### 9. File Upload Consumer (`file_upload_consumer.py`)
- Consumes uploaded and processed files
- Displays file analysis results
- Supports file saving to local disk
- Multiple consumption modes (uploaded, processed, aggregations)

**Key Concepts:**
- File reconstruction from base64
- Analysis result visualization
- Local file storage
- Multiple consumption patterns

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

#### Transaction Processing Topics
- `transactions`: Raw transaction data
- `processed_transactions`: Enriched and filtered data
- `user_aggregations`: User-wise spending summaries
- `high_value_transactions`: Transactions > $500

#### Video Processing Topics
- `video_frames`: Raw video frame data
- `processed_video_frames`: Analyzed and enriched frame data
- `video_source_aggregations`: Source-wise frame statistics

#### File Upload Processing Topics
- `uploaded_documents`: Raw document file data
- `uploaded_videos`: Raw video file data
- `uploaded_images`: Raw image file data
- `processed_files`: Analyzed and enriched file data
- `file_type_aggregations`: File type-wise statistics

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
