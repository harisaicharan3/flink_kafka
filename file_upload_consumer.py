#!/usr/bin/env python3
"""
File Upload Consumer - Consumes uploaded files from Kafka and displays processing results
Demonstrates handling uploaded files from the web UI
"""

import json
import base64
import os
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import argparse

class FileUploadConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}
        self.file_count = 0
        
    def create_consumer(self, topic_name, group_id=None):
        """Create a consumer for a specific topic"""
        if group_id is None:
            group_id = f"file-consumer-group-{topic_name}"
        
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            consumer_timeout_ms=1000
        )
        
        self.consumers[topic_name] = consumer
        return consumer
    
    def save_file_content(self, file_data, output_dir="downloaded_files"):
        """Save file content to disk"""
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Decode base64 content
            file_content = base64.b64decode(file_data['file_content'])
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_{file_data['filename']}"
            filepath = os.path.join(output_dir, filename)
            
            # Write file
            with open(filepath, 'wb') as f:
                f.write(file_content)
            
            print(f"Saved file: {filepath} ({len(file_content)} bytes)")
            return True
            
        except Exception as e:
            print(f"Error saving file: {e}")
            return False
    
    def consume_uploaded_files(self, topic_name, max_files=10, save_files=False):
        """Consume uploaded files from a specific topic"""
        if topic_name not in self.consumers:
            self.create_consumer(topic_name)
        
        consumer = self.consumers[topic_name]
        print(f"\nConsuming uploaded files from topic '{topic_name}'...")
        print("=" * 60)
        
        file_count = 0
        
        try:
            for message in consumer:
                if file_count >= max_files:
                    print(f"Reached maximum files ({max_files}), stopping")
                    break
                
                file_data = message.value
                file_id = file_data.get('file_id', 'unknown')
                
                print(f"Received file: {file_id}")
                print(f"  Filename: {file_data.get('filename', 'unknown')}")
                print(f"  File Type: {file_data.get('file_type', 'unknown')}")
                print(f"  File Size: {file_data.get('file_size', 0):,} bytes")
                print(f"  Upload Time: {file_data.get('upload_timestamp', 'unknown')}")
                print(f"  File Hash: {file_data.get('file_hash', 'unknown')}")
                
                # Display metadata
                metadata = file_data.get('metadata', {})
                if metadata:
                    print(f"  Metadata:")
                    for key, value in metadata.items():
                        if key != 'text_content':  # Skip large text content
                            if isinstance(value, str) and len(value) > 100:
                                print(f"    {key}: {value[:100]}...")
                            else:
                                print(f"    {key}: {value}")
                
                # Save file if requested
                if save_files:
                    self.save_file_content(file_data)
                
                file_count += 1
                print(f"  Processed file {file_count}")
                print("-" * 40)
                
        except Exception as e:
            print(f"Error consuming files: {e}")
        
        print(f"Consumed {file_count} files from '{topic_name}'")
        return file_count
    
    def consume_processed_files(self, topic_name, max_files=10):
        """Consume processed files and display analysis results"""
        if topic_name not in self.consumers:
            self.create_consumer(topic_name)
        
        consumer = self.consumers[topic_name]
        print(f"\nConsuming processed files from topic '{topic_name}'...")
        print("=" * 60)
        
        file_count = 0
        
        try:
            for message in consumer:
                if file_count >= max_files:
                    break
                
                file_data = message.value
                file_id = file_data.get('file_id', 'unknown')
                
                print(f"Processed file: {file_id}")
                print(f"  Filename: {file_data.get('filename', 'unknown')}")
                print(f"  File Type: {file_data.get('file_type', 'unknown')}")
                print(f"  Processing Time: {file_data.get('flink_processing_time', 'unknown')}")
                
                # Display analysis results
                analysis = file_data.get('analysis', {})
                if analysis:
                    print(f"  Analysis Results:")
                    print(f"    Size Category: {analysis.get('size_category', 'unknown')}")
                    print(f"    File Size: {analysis.get('file_size_mb', 0):.2f} MB")
                    print(f"    Content Hash: {analysis.get('content_hash', 'unknown')}")
                    
                    # Type-specific analysis
                    if file_data.get('file_type') == 'document':
                        print(f"    Text Length: {analysis.get('text_length', 0)} characters")
                        print(f"    Word Count: {analysis.get('word_count', 0)} words")
                        print(f"    Has Text: {analysis.get('has_text', False)}")
                    elif file_data.get('file_type') == 'video':
                        print(f"    Video Quality: {analysis.get('video_quality', 'unknown')}")
                        print(f"    Aspect Ratio: {analysis.get('aspect_ratio', 0)}")
                        print(f"    Total Frames: {analysis.get('total_frames', 0)}")
                        print(f"    Bitrate Estimate: {analysis.get('bitrate_estimate', 0)} bps")
                    elif file_data.get('file_type') == 'image':
                        print(f"    Image Quality: {analysis.get('image_quality', 'unknown')}")
                        print(f"    Aspect Ratio: {analysis.get('aspect_ratio', 0)}")
                        print(f"    Total Pixels: {analysis.get('total_pixels', 0):,}")
                        print(f"    Color Depth: {analysis.get('color_depth', 0)} bits")
                
                # Display processing notes
                processing_notes = file_data.get('processing_notes', '')
                if processing_notes:
                    print(f"  Processing Notes: {processing_notes}")
                
                file_count += 1
                print(f"  Processed file {file_count}")
                print("-" * 40)
                
        except Exception as e:
            print(f"Error consuming processed files: {e}")
        
        print(f"Consumed {file_count} processed files from '{topic_name}'")
        return file_count
    
    def consume_file_aggregations(self, topic_name, max_aggregations=10):
        """Consume file type aggregations"""
        if topic_name not in self.consumers:
            self.create_consumer(topic_name)
        
        consumer = self.consumers[topic_name]
        print(f"\nConsuming file aggregations from topic '{topic_name}'...")
        print("=" * 60)
        
        aggregation_count = 0
        
        try:
            for message in consumer:
                if aggregation_count >= max_aggregations:
                    break
                
                aggregation_data = message.value
                file_type = aggregation_data.get('file_type', 'unknown')
                
                print(f"File Type Aggregation: {file_type}")
                print(f"  Total Files: {aggregation_data.get('total_files', 0)}")
                print(f"  Total Size: {aggregation_data.get('total_size_bytes', 0):,} bytes")
                print(f"  Average Size: {aggregation_data.get('avg_size_bytes', 0):,.0f} bytes")
                print(f"  Largest File: {aggregation_data.get('largest_file_size', 0):,} bytes")
                print(f"  Smallest File: {aggregation_data.get('smallest_file_size', 0):,} bytes")
                print(f"  Last Updated: {aggregation_data.get('last_updated', 'unknown')}")
                
                aggregation_count += 1
                print(f"  Processed aggregation {aggregation_count}")
                print("-" * 40)
                
        except Exception as e:
            print(f"Error consuming aggregations: {e}")
        
        print(f"Consumed {aggregation_count} aggregations from '{topic_name}'")
        return aggregation_count
    
    def consume_continuous(self, topic_name, duration=60):
        """Continuously consume files for a specified duration"""
        if topic_name not in self.consumers:
            self.create_consumer(topic_name)
        
        consumer = self.consumers[topic_name]
        print(f"\nContinuously consuming from topic '{topic_name}' for {duration} seconds...")
        print("=" * 60)
        
        start_time = datetime.now()
        file_count = 0
        
        try:
            for message in consumer:
                if (datetime.now() - start_time).total_seconds() > duration:
                    print(f"Duration reached ({duration}s), stopping consumption")
                    break
                
                file_data = message.value
                current_time = datetime.now().strftime("%H:%M:%S")
                
                print(f"[{current_time}] File: {file_data.get('file_id', 'N/A')} - "
                      f"{file_data.get('filename', 'N/A')} - "
                      f"{file_data.get('file_type', 'unknown')} - "
                      f"{file_data.get('file_size', 0):,} bytes")
                
                file_count += 1
                
        except Exception as e:
            print(f"Error in continuous consumption: {e}")
        
        print(f"Consumed {file_count} files in {duration} seconds")
        return file_count
    
    def close_all(self):
        """Close all consumers"""
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
                print(f"Closed consumer for topic: {topic}")
            except Exception as e:
                print(f"Error closing consumer for {topic}: {e}")

def demonstrate_file_consumer_operations():
    """Demonstrate various file consumer operations"""
    consumer = FileUploadConsumer()
    
    try:
        print("File Upload Consumer Demo")
        print("========================")
        
        # 1. Consume uploaded documents
        print("\n1. Consuming uploaded documents...")
        consumer.consume_uploaded_files('uploaded_documents', max_files=3)
        
        # 2. Consume uploaded videos
        print("\n2. Consuming uploaded videos...")
        consumer.consume_uploaded_files('uploaded_videos', max_files=3)
        
        # 3. Consume uploaded images
        print("\n3. Consuming uploaded images...")
        consumer.consume_uploaded_files('uploaded_images', max_files=3)
        
        # 4. Consume processed files
        print("\n4. Consuming processed files...")
        consumer.consume_processed_files('processed_files', max_files=5)
        
        # 5. Consume file aggregations
        print("\n5. Consuming file aggregations...")
        consumer.consume_file_aggregations('file_type_aggregations', max_aggregations=3)
        
        # 6. Continuous consumption
        print("\n6. Continuous consumption demo (30 seconds)...")
        consumer.consume_continuous('processed_files', duration=30)
        
    except KeyboardInterrupt:
        print("\nStopping file consumer demo...")
    finally:
        consumer.close_all()

def main():
    parser = argparse.ArgumentParser(description='File Upload Consumer for Kafka')
    parser.add_argument('--topic', default='uploaded_documents', help='Kafka topic name')
    parser.add_argument('--mode', choices=['uploaded', 'processed', 'aggregations', 'continuous', 'demo'], 
                       default='demo', help='Consumer mode')
    parser.add_argument('--max-files', type=int, default=10, help='Maximum files to process')
    parser.add_argument('--duration', type=int, default=60, help='Duration for continuous mode')
    parser.add_argument('--save-files', action='store_true', help='Save files to disk')
    
    args = parser.parse_args()
    
    consumer = FileUploadConsumer()
    
    try:
        print("File Upload Consumer")
        print("===================")
        print(f"Topic: {args.topic}")
        print(f"Mode: {args.mode}")
        
        if args.mode == 'uploaded':
            consumer.consume_uploaded_files(args.topic, args.max_files, args.save_files)
        elif args.mode == 'processed':
            consumer.consume_processed_files(args.topic, args.max_files)
        elif args.mode == 'aggregations':
            consumer.consume_file_aggregations(args.topic, args.max_files)
        elif args.mode == 'continuous':
            consumer.consume_continuous(args.topic, args.duration)
        elif args.mode == 'demo':
            demonstrate_file_consumer_operations()
        
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close_all()

if __name__ == "__main__":
    main()
