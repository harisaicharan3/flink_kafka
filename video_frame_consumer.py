#!/usr/bin/env python3
"""
Video Frame Consumer - Consumes video frames from Kafka and displays/processes them
Demonstrates handling binary data (video frames) from Kafka
"""

import cv2
import json
import base64
import numpy as np
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import argparse
import os

class VideoFrameConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}
        self.frame_count = 0
        
    def create_consumer(self, topic_name, group_id=None):
        """Create a consumer for a specific topic"""
        if group_id is None:
            group_id = f"video-consumer-group-{topic_name}"
        
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
    
    def base64_to_frame(self, frame_base64):
        """Convert base64 string back to OpenCV frame"""
        try:
            # Decode base64
            frame_bytes = base64.b64decode(frame_base64)
            
            # Convert to numpy array
            nparr = np.frombuffer(frame_bytes, np.uint8)
            
            # Decode image
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            return frame
        except Exception as e:
            print(f"Error decoding frame: {e}")
            return None
    
    def process_frame(self, frame, frame_data):
        """Process a video frame (add effects, analysis, etc.)"""
        if frame is None:
            return None
        
        processed_frame = frame.copy()
        
        # Add processing timestamp
        timestamp = datetime.now().strftime("%H:%M:%S")
        cv2.putText(processed_frame, f"Processed: {timestamp}", 
                   (10, processed_frame.shape[0] - 20), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
        
        # Add frame info
        frame_id = frame_data.get('frame_id', 'unknown')
        cv2.putText(processed_frame, f"ID: {frame_id}", 
                   (10, 30), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
        
        # Add frame count
        self.frame_count += 1
        cv2.putText(processed_frame, f"Count: {self.frame_count}", 
                   (10, 60), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
        
        # Add some visual effects
        if self.frame_count % 30 == 0:  # Every 30 frames
            # Add a pulsing circle
            center = (processed_frame.shape[1]//2, processed_frame.shape[0]//2)
            radius = int(50 + 20 * np.sin(time.time() * 5))
            cv2.circle(processed_frame, center, radius, (0, 0, 255), 3)
        
        return processed_frame
    
    def save_frame(self, frame, frame_data, output_dir="output_frames"):
        """Save frame to disk"""
        if frame is None:
            return False
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename
        frame_id = frame_data.get('frame_id', f"frame_{int(time.time())}")
        filename = f"{frame_id}.jpg"
        filepath = os.path.join(output_dir, filename)
        
        try:
            cv2.imwrite(filepath, frame)
            print(f"Saved frame: {filepath}")
            return True
        except Exception as e:
            print(f"Error saving frame: {e}")
            return False
    
    def consume_and_display(self, topic_name, max_frames=100, display=True, save=False):
        """Consume frames and display them"""
        if topic_name not in self.consumers:
            self.create_consumer(topic_name)
        
        consumer = self.consumers[topic_name]
        print(f"\nConsuming video frames from topic '{topic_name}'...")
        print("Press 'q' to quit, 's' to save current frame")
        print("=" * 50)
        
        frame_count = 0
        
        try:
            for message in consumer:
                if frame_count >= max_frames:
                    print(f"Reached maximum frames ({max_frames}), stopping")
                    break
                
                frame_data = message.value
                frame_id = frame_data.get('frame_id', 'unknown')
                
                print(f"Received frame: {frame_id}")
                print(f"  Size: {frame_data.get('frame_width', 0)}x{frame_data.get('frame_height', 0)}")
                print(f"  Source: {frame_data.get('source', 'unknown')}")
                print(f"  Timestamp: {frame_data.get('timestamp', 'unknown')}")
                
                # Decode frame
                frame = self.base64_to_frame(frame_data['frame_data'])
                if frame is None:
                    print("  Error: Could not decode frame")
                    continue
                
                # Process frame
                processed_frame = self.process_frame(frame, frame_data)
                
                # Display frame
                if display:
                    cv2.imshow('Video Frame Consumer', processed_frame)
                    
                    # Handle key presses
                    key = cv2.waitKey(1) & 0xFF
                    if key == ord('q'):
                        print("Quit requested")
                        break
                    elif key == ord('s'):
                        self.save_frame(processed_frame, frame_data)
                
                # Save frame if requested
                if save:
                    self.save_frame(processed_frame, frame_data)
                
                frame_count += 1
                print(f"  Processed frame {frame_count}")
                print("-" * 30)
                
        except Exception as e:
            print(f"Error consuming frames: {e}")
        
        if display:
            cv2.destroyAllWindows()
        
        print(f"Consumed {frame_count} frames from '{topic_name}'")
        return frame_count
    
    def consume_and_analyze(self, topic_name, max_frames=50):
        """Consume frames and perform basic analysis"""
        if topic_name not in self.consumers:
            self.create_consumer(topic_name)
        
        consumer = self.consumers[topic_name]
        print(f"\nAnalyzing video frames from topic '{topic_name}'...")
        print("=" * 50)
        
        frame_count = 0
        total_brightness = 0
        frame_sizes = []
        
        try:
            for message in consumer:
                if frame_count >= max_frames:
                    break
                
                frame_data = message.value
                frame = self.base64_to_frame(frame_data['frame_data'])
                
                if frame is not None:
                    # Calculate brightness
                    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                    brightness = np.mean(gray)
                    total_brightness += brightness
                    
                    # Store frame size
                    frame_sizes.append((frame_data.get('frame_width', 0), 
                                      frame_data.get('frame_height', 0)))
                    
                    print(f"Frame {frame_count + 1}: Brightness = {brightness:.2f}, "
                          f"Size = {frame_data.get('frame_width', 0)}x{frame_data.get('frame_height', 0)}")
                
                frame_count += 1
                
        except Exception as e:
            print(f"Error analyzing frames: {e}")
        
        # Print analysis results
        if frame_count > 0:
            avg_brightness = total_brightness / frame_count
            print(f"\nAnalysis Results:")
            print(f"  Total frames analyzed: {frame_count}")
            print(f"  Average brightness: {avg_brightness:.2f}")
            print(f"  Frame sizes: {set(frame_sizes)}")
        
        return frame_count
    
    def consume_continuous(self, topic_name, duration=60):
        """Continuously consume frames for a specified duration"""
        if topic_name not in self.consumers:
            self.create_consumer(topic_name)
        
        consumer = self.consumers[topic_name]
        print(f"\nContinuously consuming from topic '{topic_name}' for {duration} seconds...")
        print("=" * 50)
        
        start_time = time.time()
        frame_count = 0
        
        try:
            for message in consumer:
                if time.time() - start_time > duration:
                    print(f"Duration reached ({duration}s), stopping consumption")
                    break
                
                frame_data = message.value
                current_time = datetime.now().strftime("%H:%M:%S")
                
                print(f"[{current_time}] Frame: {frame_data.get('frame_id', 'N/A')} - "
                      f"{frame_data.get('frame_width', 0)}x{frame_data.get('frame_height', 0)} - "
                      f"Source: {frame_data.get('source', 'unknown')}")
                
                frame_count += 1
                
        except Exception as e:
            print(f"Error in continuous consumption: {e}")
        
        print(f"Consumed {frame_count} frames in {duration} seconds")
        return frame_count
    
    def close_all(self):
        """Close all consumers"""
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
                print(f"Closed consumer for topic: {topic}")
            except Exception as e:
                print(f"Error closing consumer for {topic}: {e}")

def demonstrate_video_consumer_operations():
    """Demonstrate various video consumer operations"""
    consumer = VideoFrameConsumer()
    
    try:
        print("Video Frame Consumer Demo")
        print("========================")
        
        # 1. Analyze frames
        print("\n1. Analyzing video frames...")
        consumer.consume_and_analyze('video_frames', max_frames=10)
        
        # 2. Display frames (if OpenCV display is available)
        print("\n2. Displaying video frames...")
        print("Note: This requires a display. If running headless, frames will be processed but not displayed.")
        consumer.consume_and_display('video_frames', max_frames=5, display=True, save=False)
        
        # 3. Save frames
        print("\n3. Saving video frames...")
        consumer.consume_and_display('video_frames', max_frames=5, display=False, save=True)
        
        # 4. Continuous consumption
        print("\n4. Continuous consumption demo (30 seconds)...")
        consumer.consume_continuous('video_frames', duration=30)
        
    except KeyboardInterrupt:
        print("\nStopping video consumer demo...")
    finally:
        consumer.close_all()

def main():
    parser = argparse.ArgumentParser(description='Video Frame Consumer for Kafka')
    parser.add_argument('--topic', default='video_frames', help='Kafka topic name')
    parser.add_argument('--mode', choices=['display', 'analyze', 'save', 'continuous', 'demo'], 
                       default='demo', help='Consumer mode')
    parser.add_argument('--max-frames', type=int, default=100, help='Maximum frames to process')
    parser.add_argument('--duration', type=int, default=60, help='Duration for continuous mode')
    parser.add_argument('--output-dir', default='output_frames', help='Output directory for saved frames')
    
    args = parser.parse_args()
    
    consumer = VideoFrameConsumer()
    
    try:
        print("Video Frame Consumer")
        print("===================")
        print(f"Topic: {args.topic}")
        print(f"Mode: {args.mode}")
        
        if args.mode == 'display':
            consumer.consume_and_display(args.topic, args.max_frames, display=True, save=False)
        elif args.mode == 'analyze':
            consumer.consume_and_analyze(args.topic, args.max_frames)
        elif args.mode == 'save':
            consumer.consume_and_display(args.topic, args.max_frames, display=False, save=True)
        elif args.mode == 'continuous':
            consumer.consume_continuous(args.topic, args.duration)
        elif args.mode == 'demo':
            demonstrate_video_consumer_operations()
        
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close_all()

if __name__ == "__main__":
    main()
