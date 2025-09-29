#!/usr/bin/env python3
"""
Flink File Processing Job
Demonstrates processing uploaded files (documents, videos, images) with Flink operations
"""

import json
import base64
import hashlib
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, FilterFunction, KeyedProcessFunction
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import ReduceFunction
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

class UploadedFileData:
    """Data class for uploaded file records"""
    def __init__(self, file_id, filename, file_type, file_size, file_hash, 
                 upload_timestamp, file_content, metadata):
        self.file_id = file_id
        self.filename = filename
        self.file_type = file_type
        self.file_size = file_size
        self.file_hash = file_hash
        self.upload_timestamp = upload_timestamp
        self.file_content = file_content
        self.metadata = metadata
    
    def to_dict(self):
        return {
            'file_id': self.file_id,
            'filename': self.filename,
            'file_type': self.file_type,
            'file_size': self.file_size,
            'file_hash': self.file_hash,
            'upload_timestamp': self.upload_timestamp,
            'file_content': self.file_content,
            'metadata': self.metadata
        }

class JsonToFileDataMap(MapFunction):
    """Map function to convert JSON string to UploadedFileData object"""
    def map(self, value):
        try:
            data = json.loads(value)
            return UploadedFileData(
                file_id=data['file_id'],
                filename=data['filename'],
                file_type=data['file_type'],
                file_size=data['file_size'],
                file_hash=data['file_hash'],
                upload_timestamp=data['upload_timestamp'],
                file_content=data['file_content'],
                metadata=data.get('metadata', {})
            )
        except Exception as e:
            print(f"Error parsing uploaded file data: {e}")
            return None

class LargeFileFilter(FilterFunction):
    """Filter function to keep only files larger than 1MB"""
    def filter(self, value):
        if value is None:
            return False
        return value.file_size > 1024 * 1024  # 1MB

class FileAnalysisMap(MapFunction):
    """Map function to analyze file properties"""
    def map(self, value):
        if value is None:
            return None
        
        try:
            # Basic file analysis
            file_size_mb = value.file_size / (1024 * 1024)
            
            # Determine file category based on size
            if file_size_mb < 1:
                size_category = 'small'
            elif file_size_mb < 10:
                size_category = 'medium'
            elif file_size_mb < 100:
                size_category = 'large'
            else:
                size_category = 'very_large'
            
            # Create enriched file data
            enriched_data = value.to_dict()
            enriched_data['analysis'] = {
                'file_size_mb': round(file_size_mb, 2),
                'size_category': size_category,
                'processing_timestamp': datetime.now().isoformat(),
                'content_hash': hashlib.sha256(value.file_content.encode()).hexdigest()[:16]
            }
            
            # Add type-specific analysis
            if value.file_type == 'document':
                text_content = value.metadata.get('text_content', '')
                enriched_data['analysis']['text_length'] = len(text_content)
                enriched_data['analysis']['word_count'] = len(text_content.split())
                enriched_data['analysis']['has_text'] = len(text_content.strip()) > 0
                
            elif value.file_type == 'video':
                fps = value.metadata.get('fps', 0)
                duration = value.metadata.get('duration', 0)
                width = value.metadata.get('width', 0)
                height = value.metadata.get('height', 0)
                
                enriched_data['analysis']['video_quality'] = 'HD' if width >= 1280 else 'SD'
                enriched_data['analysis']['aspect_ratio'] = round(width / height, 2) if height > 0 else 0
                enriched_data['analysis']['total_frames'] = value.metadata.get('frame_count', 0)
                enriched_data['analysis']['bitrate_estimate'] = round((value.file_size * 8) / duration, 2) if duration > 0 else 0
                
            elif value.file_type == 'image':
                width = value.metadata.get('width', 0)
                height = value.metadata.get('height', 0)
                channels = value.metadata.get('channels', 0)
                
                enriched_data['analysis']['image_quality'] = 'High' if width >= 1920 else 'Medium' if width >= 1280 else 'Low'
                enriched_data['analysis']['aspect_ratio'] = round(width / height, 2) if height > 0 else 0
                enriched_data['analysis']['total_pixels'] = width * height
                enriched_data['analysis']['color_depth'] = channels * 8  # Assuming 8 bits per channel
            
            return enriched_data
            
        except Exception as e:
            print(f"Error analyzing file: {e}")
            return value.to_dict()

class FileTypeEnrichmentMap(MapFunction):
    """Map function to add file type-specific processing"""
    def map(self, value):
        if value is None:
            return None
        
        enriched_data = value.copy()
        
        # Add processing notes based on file type
        file_type = enriched_data.get('file_type', 'unknown')
        
        if file_type == 'document':
            enriched_data['processing_notes'] = 'Document processing - text extraction completed'
            enriched_data['processing_priority'] = 'normal'
        elif file_type == 'video':
            enriched_data['processing_notes'] = 'Video processing - metadata extraction completed'
            enriched_data['processing_priority'] = 'high'
        elif file_type == 'image':
            enriched_data['processing_notes'] = 'Image processing - metadata extraction completed'
            enriched_data['processing_priority'] = 'normal'
        else:
            enriched_data['processing_notes'] = 'Unknown file type processing'
            enriched_data['processing_priority'] = 'low'
        
        # Add processing timestamp
        enriched_data['flink_processing_time'] = datetime.now().isoformat()
        
        return enriched_data

class FileDataToJsonMap(MapFunction):
    """Map function to convert file data back to JSON string"""
    def map(self, value):
        if value is None:
            return "{}"
        return json.dumps(value)

class FileTypeAggregator(KeyedProcessFunction):
    """Keyed process function to aggregate files by type"""
    def __init__(self):
        self.file_type_stats = {}
    
    def process_element(self, value, ctx):
        if value is None:
            return
        
        file_type = value['file_type']
        
        if file_type not in self.file_type_stats:
            self.file_type_stats[file_type] = {
                'file_type': file_type,
                'total_files': 0,
                'total_size_bytes': 0,
                'avg_size_bytes': 0,
                'largest_file_size': 0,
                'smallest_file_size': float('inf'),
                'last_updated': datetime.now().isoformat()
            }
        
        stats = self.file_type_stats[file_type]
        file_size = value.get('file_size', 0)
        
        stats['total_files'] += 1
        stats['total_size_bytes'] += file_size
        
        # Update size statistics
        stats['largest_file_size'] = max(stats['largest_file_size'], file_size)
        stats['smallest_file_size'] = min(stats['smallest_file_size'], file_size)
        stats['avg_size_bytes'] = stats['total_size_bytes'] / stats['total_files']
        
        stats['last_updated'] = datetime.now().isoformat()
        
        # Output the updated file type summary
        return json.dumps(stats)

def create_file_processing_flink_job():
    """Create and configure the Flink file processing job"""
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism for demo purposes
    
    # Add Kafka connector JARs
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.18.0.jar")
    
    # Define Kafka consumer properties
    kafka_consumer_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-file-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    
    # Define Kafka producer properties
    kafka_producer_props = {
        'bootstrap.servers': 'localhost:9092'
    }
    
    # Create Kafka consumers for different file types
    document_consumer = FlinkKafkaConsumer(
        topics='uploaded_documents',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_consumer_props
    )
    
    video_consumer = FlinkKafkaConsumer(
        topics='uploaded_videos',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_consumer_props
    )
    
    image_consumer = FlinkKafkaConsumer(
        topics='uploaded_images',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_consumer_props
    )
    
    # Create Kafka producers for processed files
    processed_files_producer = FlinkKafkaProducer(
        topic='processed_files',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_producer_props
    )
    
    # Create Kafka producer for file type aggregations
    file_type_aggregation_producer = FlinkKafkaProducer(
        topic='file_type_aggregations',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_producer_props
    )
    
    # Main processing pipeline
    print("Setting up Flink file processing pipeline...")
    
    # 1. Read uploaded files from Kafka (combine all file type topics)
    document_stream = env.add_source(document_consumer)
    video_stream = env.add_source(video_consumer)
    image_stream = env.add_source(image_consumer)
    
    # Union all file streams
    all_files_stream = document_stream.union(video_stream).union(image_stream)
    
    # 2. Parse JSON to UploadedFileData objects
    parsed_stream = all_files_stream.map(JsonToFileDataMap())
    
    # 3. Filter large files only
    large_files_stream = parsed_stream.filter(LargeFileFilter())
    
    # 4. Analyze file properties
    analyzed_stream = large_files_stream.map(FileAnalysisMap())
    
    # 5. Add file type enrichment
    enriched_stream = analyzed_stream.map(FileTypeEnrichmentMap())
    
    # 6. Convert back to JSON and write to processed_files topic
    json_stream = enriched_stream.map(FileDataToJsonMap())
    json_stream.add_sink(processed_files_producer)
    
    # 7. Create file type aggregation stream
    file_type_stream = enriched_stream.key_by(lambda x: x['file_type']) \
        .process(FileTypeAggregator())
    
    # 8. Write file type aggregations to separate topic
    file_type_stream.add_sink(file_type_aggregation_producer)
    
    # 9. Windowed aggregation example - file size statistics by category every 30 seconds
    size_category_stream = enriched_stream.key_by(lambda x: x.get('analysis', {}).get('size_category', 'unknown')) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .reduce(lambda a, b: {
            'size_category': a.get('analysis', {}).get('size_category', 'unknown'),
            'total_files': a.get('total_files', 1) + 1,
            'total_size_bytes': a.get('total_size_bytes', a.get('file_size', 0)) + b.get('file_size', 0),
            'avg_size_bytes': (a.get('avg_size_bytes', a.get('file_size', 0)) + b.get('file_size', 0)) / 2,
            'window_end': datetime.now().isoformat()
        })
    
    # Convert size category aggregations to JSON and print (for demo)
    size_category_json_stream = size_category_stream.map(lambda x: json.dumps(x))
    size_category_json_stream.print("File Size Category Aggregations")
    
    return env

def main():
    """Main function to run the Flink file processing job"""
    print("Starting Flink File Processing Job")
    print("==================================")
    
    try:
        # Create and execute the Flink job
        env = create_file_processing_flink_job()
        
        print("Job configuration complete. Starting execution...")
        print("The job will process uploaded files from topics:")
        print("- uploaded_documents")
        print("- uploaded_videos") 
        print("- uploaded_images")
        print("and write results to 'processed_files' and 'file_type_aggregations' topics")
        print("Press Ctrl+C to stop the job")
        
        # Execute the job
        env.execute("Kafka-Flink File Processing Job")
        
    except Exception as e:
        print(f"Error running Flink file job: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
