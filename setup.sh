#!/bin/bash

# Setup script for Flink-Kafka learning project
echo "Setting up Flink-Kafka Learning Environment"
echo "=========================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create Python virtual environment
echo "Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Start Docker services
echo "Starting Kafka and Flink services..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Check if Kafka is ready
echo "Checking Kafka status..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Create Kafka topics
echo "Creating Kafka topics..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic transactions --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic processed_transactions --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic user_aggregations --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic high_value_transactions --partitions 3 --replication-factor 1

# Create video processing topics
echo "Creating video processing topics..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic video_frames --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic processed_video_frames --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic video_source_aggregations --partitions 3 --replication-factor 1

# List created topics
echo "Created topics:"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "Setup complete!"
echo "==============="
echo "Services running:"
echo "- Kafka: localhost:9092"
echo "- Kafka UI: http://localhost:8080"
echo "- Flink Dashboard: http://localhost:8081"
echo ""
echo "Next steps:"
echo "1. Activate virtual environment: source venv/bin/activate"
echo "2. Run transaction producer: python kafka_producer.py"
echo "3. Run transaction Flink job: python flink_processor.py"
echo "4. Run transaction consumer: python kafka_consumer.py"
echo ""
echo "Video processing:"
echo "5. Run video producer: python video_frame_producer.py"
echo "6. Run video Flink job: python video_flink_processor.py"
echo "7. Run video consumer: python video_frame_consumer.py"
echo ""
echo "To stop services: docker-compose down"
