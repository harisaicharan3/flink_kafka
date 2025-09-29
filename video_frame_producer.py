#!/usr/bin/env python3
"""
Video Frame Producer - Captures video frames and sends them to Kafka
Demonstrates handling binary data (video frames) with Kafka
"""

import cv2
import json
import time
import base64
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import argparse
import os

class VideoFrameProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        self.frame_count = 0
        
    def capture_frame_from_webcam(self):
        """Capture a frame from the default webcam"""
        cap = cv2.VideoCapture(0)
        
        if not cap.isOpened():
            print("Error: Could not open webcam")
            return None
        
        ret, frame = cap.read()
        cap.release()
        
        if ret:
            return frame
        else:
            print("Error: Could not capture frame from webcam")
            return None
    
    def capture_frame_from_video(self, video_path):
        """Capture frames from a video file"""
        cap = cv2.VideoCapture(video_path)
        
        if not cap.isOpened():
            print(f"Error: Could not open video file {video_path}")
            return None
        
        ret, frame = cap.read()
        cap.release()
        
        if ret:
            return frame
        else:
            print(f"Error: Could not capture frame from video {video_path}")
            return None
    
    def generate_sample_frame(self, width=640, height=480):
        """Generate a sample frame with text overlay for demo purposes"""
        # Create a blank frame
        frame = np.zeros((height, width, 3), dtype=np.uint8)
        
        # Add some color gradient
        for i in range(height):
            frame[i, :, 0] = int(255 * i / height)  # Red gradient
            frame[i, :, 1] = int(255 * (height - i) / height)  # Green gradient
        
        # Add text overlay
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        text = f"Frame {self.frame_count} - {timestamp}"
        
        cv2.putText(frame, text, (50, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
        cv2.putText(frame, f"Size: {width}x{height}", (50, 100), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
        
        # Add a moving circle for animation effect
        center_x = int(width/2 + 100 * np.sin(time.time()))
        center_y = int(height/2 + 50 * np.cos(time.time()))
        cv2.circle(frame, (center_x, center_y), 30, (0, 255, 255), -1)
        
        return frame
    
    def frame_to_base64(self, frame):
        """Convert OpenCV frame to base64 string"""
        if frame is None:
            return None
        
        # Encode frame as JPEG
        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
        
        # Convert to base64
        frame_base64 = base64.b64encode(buffer).decode('utf-8')
        return frame_base64
    
    def create_frame_message(self, frame, source="webcam", metadata=None):
        """Create a message containing frame data and metadata"""
        if frame is None:
            return None
        
        self.frame_count += 1
        
        # Convert frame to base64
        frame_base64 = self.frame_to_base64(frame)
        if frame_base64 is None:
            return None
        
        # Create message
        message = {
            'frame_id': f"frame_{self.frame_count}_{int(time.time() * 1000)}",
            'timestamp': datetime.now().isoformat(),
            'source': source,
            'frame_data': frame_base64,
            'frame_width': frame.shape[1],
            'frame_height': frame.shape[0],
            'frame_channels': frame.shape[2] if len(frame.shape) > 2 else 1,
            'frame_size_bytes': len(frame_base64.encode('utf-8')),
            'metadata': metadata or {}
        }
        
        return message
    
    def send_frame_to_topic(self, topic_name, frame, source="webcam", key=None):
        """Send frame data to Kafka topic"""
        message = self.create_frame_message(frame, source)
        if message is None:
            return False
        
        try:
            future = self.producer.send(topic_name, value=message, key=key)
            record_metadata = future.get(timeout=10)
            print(f"Sent frame {message['frame_id']} to topic: {topic_name}, "
                  f"partition: {record_metadata.partition}, offset: {record_metadata.offset}")
            return True
        except KafkaError as e:
            print(f"Failed to send frame: {e}")
            return False
    
    def produce_from_webcam(self, topic_name, duration=60, interval=1):
        """Continuously capture and send frames from webcam"""
        print(f"Starting webcam capture for {duration} seconds...")
        print("Press Ctrl+C to stop early")
        
        start_time = time.time()
        frame_count = 0
        
        try:
            while time.time() - start_time < duration:
                frame = self.capture_frame_from_webcam()
                if frame is not None:
                    success = self.send_frame_to_topic(topic_name, frame, source="webcam")
                    if success:
                        frame_count += 1
                        print(f"Captured frame {frame_count} from webcam")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\nWebcam capture stopped by user")
        
        print(f"Captured {frame_count} frames from webcam")
    
    def produce_from_video_file(self, topic_name, video_path, interval=0.1):
        """Capture and send frames from a video file"""
        print(f"Processing video file: {video_path}")
        
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            print(f"Error: Could not open video file {video_path}")
            return
        
        frame_count = 0
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_interval = max(1, int(fps * interval))  # Convert interval to frame count
        
        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                
                # Skip frames based on interval
                if frame_count % frame_interval == 0:
                    success = self.send_frame_to_topic(topic_name, frame, source="video_file", 
                                                     metadata={'video_path': video_path})
                    if success:
                        print(f"Processed frame {frame_count} from video")
                
                frame_count += 1
                
        except KeyboardInterrupt:
            print("\nVideo processing stopped by user")
        finally:
            cap.release()
        
        print(f"Processed {frame_count} frames from video file")
    
    def produce_sample_frames(self, topic_name, count=100, interval=0.5):
        """Generate and send sample frames for demo purposes"""
        print(f"Generating {count} sample frames...")
        
        for i in range(count):
            frame = self.generate_sample_frame()
            if frame is not None:
                success = self.send_frame_to_topic(topic_name, frame, source="sample_generator")
                if success:
                    print(f"Generated sample frame {i+1}/{count}")
            
            time.sleep(interval)
        
        print("Sample frame generation complete")
    
    def close(self):
        """Close the producer"""
        self.producer.close()

def main():
    parser = argparse.ArgumentParser(description='Video Frame Producer for Kafka')
    parser.add_argument('--topic', default='video_frames', help='Kafka topic name')
    parser.add_argument('--source', choices=['webcam', 'video', 'sample'], default='sample',
                       help='Video source type')
    parser.add_argument('--video-path', help='Path to video file (required for video source)')
    parser.add_argument('--duration', type=int, default=60, help='Duration in seconds (for webcam)')
    parser.add_argument('--interval', type=float, default=1.0, help='Interval between frames in seconds')
    parser.add_argument('--count', type=int, default=100, help='Number of frames (for sample)')
    
    args = parser.parse_args()
    
    producer = VideoFrameProducer()
    
    try:
        print("Video Frame Producer Demo")
        print("========================")
        print(f"Topic: {args.topic}")
        print(f"Source: {args.source}")
        
        if args.source == 'webcam':
            producer.produce_from_webcam(args.topic, args.duration, args.interval)
        elif args.source == 'video':
            if not args.video_path:
                print("Error: --video-path is required for video source")
                return
            if not os.path.exists(args.video_path):
                print(f"Error: Video file {args.video_path} does not exist")
                return
            producer.produce_from_video_file(args.topic, args.video_path, args.interval)
        elif args.source == 'sample':
            producer.produce_sample_frames(args.topic, args.count, args.interval)
        
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
