#!/usr/bin/env python3
"""
Flink Video Processing Job
Demonstrates processing video frames with Flink operations
"""

import json
import base64
import numpy as np
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

class VideoFrameData:
    """Data class for video frame records"""
    def __init__(self, frame_id, timestamp, source, frame_data, frame_width, 
                 frame_height, frame_channels, frame_size_bytes, metadata):
        self.frame_id = frame_id
        self.timestamp = timestamp
        self.source = source
        self.frame_data = frame_data
        self.frame_width = frame_width
        self.frame_height = frame_height
        self.frame_channels = frame_channels
        self.frame_size_bytes = frame_size_bytes
        self.metadata = metadata
    
    def to_dict(self):
        return {
            'frame_id': self.frame_id,
            'timestamp': self.timestamp,
            'source': self.source,
            'frame_data': self.frame_data,
            'frame_width': self.frame_width,
            'frame_height': self.frame_height,
            'frame_channels': self.frame_channels,
            'frame_size_bytes': self.frame_size_bytes,
            'metadata': self.metadata
        }

class JsonToVideoFrameMap(MapFunction):
    """Map function to convert JSON string to VideoFrameData object"""
    def map(self, value):
        try:
            data = json.loads(value)
            return VideoFrameData(
                frame_id=data['frame_id'],
                timestamp=data['timestamp'],
                source=data['source'],
                frame_data=data['frame_data'],
                frame_width=data['frame_width'],
                frame_height=data['frame_height'],
                frame_channels=data['frame_channels'],
                frame_size_bytes=data['frame_size_bytes'],
                metadata=data.get('metadata', {})
            )
        except Exception as e:
            print(f"Error parsing video frame data: {e}")
            return None

class HighResolutionFilter(FilterFunction):
    """Filter function to keep only high-resolution frames (> 480p)"""
    def filter(self, value):
        if value is None:
            return False
        return value.frame_width >= 640 and value.frame_height >= 480

class FrameAnalysisMap(MapFunction):
    """Map function to analyze frame properties"""
    def map(self, value):
        if value is None:
            return None
        
        try:
            # Decode frame to analyze it
            frame_bytes = base64.b64decode(value.frame_data)
            nparr = np.frombuffer(frame_bytes, np.uint8)
            
            # Basic analysis (simplified without OpenCV dependency in Flink)
            frame_size = len(frame_bytes)
            aspect_ratio = value.frame_width / value.frame_height
            
            # Create enriched frame data
            enriched_data = value.to_dict()
            enriched_data['analysis'] = {
                'aspect_ratio': round(aspect_ratio, 2),
                'total_pixels': value.frame_width * value.frame_height,
                'compression_ratio': round(frame_size / (value.frame_width * value.frame_height * 3), 3),
                'processing_timestamp': datetime.now().isoformat()
            }
            
            # Add quality assessment
            if aspect_ratio > 1.5:
                enriched_data['analysis']['orientation'] = 'landscape'
            elif aspect_ratio < 0.8:
                enriched_data['analysis']['orientation'] = 'portrait'
            else:
                enriched_data['analysis']['orientation'] = 'square'
            
            # Add resolution category
            total_pixels = value.frame_width * value.frame_height
            if total_pixels >= 1920 * 1080:
                enriched_data['analysis']['resolution_category'] = 'HD'
            elif total_pixels >= 1280 * 720:
                enriched_data['analysis']['resolution_category'] = 'HD_Ready'
            elif total_pixels >= 640 * 480:
                enriched_data['analysis']['resolution_category'] = 'SD'
            else:
                enriched_data['analysis']['resolution_category'] = 'Low'
            
            return enriched_data
            
        except Exception as e:
            print(f"Error analyzing frame: {e}")
            return value.to_dict()

class SourceBasedEnrichmentMap(MapFunction):
    """Map function to add source-specific metadata"""
    def map(self, value):
        if value is None:
            return None
        
        enriched_data = value.copy()
        
        # Add source-specific processing
        source = enriched_data.get('source', 'unknown')
        
        if source == 'webcam':
            enriched_data['processing_notes'] = 'Real-time webcam capture'
            enriched_data['expected_fps'] = 30
        elif source == 'video_file':
            enriched_data['processing_notes'] = 'Pre-recorded video file'
            enriched_data['expected_fps'] = 25
        elif source == 'sample_generator':
            enriched_data['processing_notes'] = 'Generated sample frame'
            enriched_data['expected_fps'] = 1
        
        # Add processing timestamp
        enriched_data['flink_processing_time'] = datetime.now().isoformat()
        
        return enriched_data

class VideoFrameToJsonMap(MapFunction):
    """Map function to convert video frame back to JSON string"""
    def map(self, value):
        if value is None:
            return "{}"
        return json.dumps(value)

class SourceAggregator(KeyedProcessFunction):
    """Keyed process function to aggregate frames by source"""
    def __init__(self):
        self.source_stats = {}
    
    def process_element(self, value, ctx):
        if value is None:
            return
        
        source = value['source']
        
        if source not in self.source_stats:
            self.source_stats[source] = {
                'source': source,
                'total_frames': 0,
                'total_size_bytes': 0,
                'avg_width': 0,
                'avg_height': 0,
                'last_updated': datetime.now().isoformat()
            }
        
        stats = self.source_stats[source]
        stats['total_frames'] += 1
        stats['total_size_bytes'] += value.get('frame_size_bytes', 0)
        
        # Update average dimensions
        current_width = value.get('frame_width', 0)
        current_height = value.get('frame_height', 0)
        
        stats['avg_width'] = ((stats['avg_width'] * (stats['total_frames'] - 1)) + current_width) / stats['total_frames']
        stats['avg_height'] = ((stats['avg_height'] * (stats['total_frames'] - 1)) + current_height) / stats['total_frames']
        
        stats['last_updated'] = datetime.now().isoformat()
        
        # Output the updated source summary
        return json.dumps(stats)

def create_video_flink_job():
    """Create and configure the Flink video processing job"""
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism for demo purposes
    
    # Add Kafka connector JARs
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.18.0.jar")
    
    # Define Kafka consumer properties
    kafka_consumer_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-video-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    
    # Define Kafka producer properties
    kafka_producer_props = {
        'bootstrap.servers': 'localhost:9092'
    }
    
    # Create Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='video_frames',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_consumer_props
    )
    
    # Create Kafka producer for processed frames
    processed_frames_producer = FlinkKafkaProducer(
        topic='processed_video_frames',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_producer_props
    )
    
    # Create Kafka producer for source aggregations
    source_aggregation_producer = FlinkKafkaProducer(
        topic='video_source_aggregations',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_producer_props
    )
    
    # Main processing pipeline
    print("Setting up Flink video processing pipeline...")
    
    # 1. Read video frames from Kafka
    video_stream = env.add_source(kafka_consumer)
    
    # 2. Parse JSON to VideoFrameData objects
    parsed_stream = video_stream.map(JsonToVideoFrameMap())
    
    # 3. Filter high-resolution frames
    high_res_stream = parsed_stream.filter(HighResolutionFilter())
    
    # 4. Analyze frame properties
    analyzed_stream = high_res_stream.map(FrameAnalysisMap())
    
    # 5. Add source-based enrichment
    enriched_stream = analyzed_stream.map(SourceBasedEnrichmentMap())
    
    # 6. Convert back to JSON and write to processed_video_frames topic
    json_stream = enriched_stream.map(VideoFrameToJsonMap())
    json_stream.add_sink(processed_frames_producer)
    
    # 7. Create source aggregation stream
    source_stream = enriched_stream.key_by(lambda x: x['source']) \
        .process(SourceAggregator())
    
    # 8. Write source aggregations to separate topic
    source_stream.add_sink(source_aggregation_producer)
    
    # 9. Windowed aggregation example - frame statistics by resolution category every 30 seconds
    resolution_stream = enriched_stream.key_by(lambda x: x.get('analysis', {}).get('resolution_category', 'unknown')) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .reduce(lambda a, b: {
            'resolution_category': a.get('analysis', {}).get('resolution_category', 'unknown'),
            'total_frames': a.get('total_frames', 1) + 1,
            'total_size_bytes': a.get('total_size_bytes', a.get('frame_size_bytes', 0)) + b.get('frame_size_bytes', 0),
            'avg_width': (a.get('avg_width', a.get('frame_width', 0)) + b.get('frame_width', 0)) / 2,
            'avg_height': (a.get('avg_height', a.get('frame_height', 0)) + b.get('frame_height', 0)) / 2,
            'window_end': datetime.now().isoformat()
        })
    
    # Convert resolution aggregations to JSON and print (for demo)
    resolution_json_stream = resolution_stream.map(lambda x: json.dumps(x))
    resolution_json_stream.print("Resolution Aggregations")
    
    return env

def main():
    """Main function to run the Flink video processing job"""
    print("Starting Flink Video Processing Job")
    print("===================================")
    
    try:
        # Create and execute the Flink job
        env = create_video_flink_job()
        
        print("Job configuration complete. Starting execution...")
        print("The job will process video frames from the 'video_frames' topic")
        print("and write results to 'processed_video_frames' and 'video_source_aggregations' topics")
        print("Press Ctrl+C to stop the job")
        
        # Execute the job
        env.execute("Kafka-Flink Video Processing Job")
        
    except Exception as e:
        print(f"Error running Flink video job: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
