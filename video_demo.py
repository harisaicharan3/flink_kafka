#!/usr/bin/env python3
"""
Video Demo Script - Demonstrates the complete video processing pipeline
Shows video frame capture, Kafka streaming, Flink processing, and consumption
"""

import time
import threading
import subprocess
import sys
import signal
import os
from datetime import datetime

class VideoProcessingDemo:
    def __init__(self):
        self.processes = []
        self.running = True
        
    def start_video_flink_job(self):
        """Start the Flink video processing job"""
        print("🎬 Starting Flink video processing job...")
        try:
            process = subprocess.Popen([
                sys.executable, 'video_flink_processor.py'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.processes.append(('Video Flink Job', process))
            print("✅ Video Flink job started")
        except Exception as e:
            print(f"❌ Failed to start video Flink job: {e}")
    
    def start_video_producer(self, duration=60, source='sample'):
        """Start the video frame producer"""
        print(f"📹 Starting video producer ({source}) for {duration} seconds...")
        try:
            cmd = [sys.executable, 'video_frame_producer.py', '--source', source]
            if source == 'sample':
                cmd.extend(['--count', '50'])
            else:
                cmd.extend(['--duration', str(duration)])
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.processes.append(('Video Producer', process))
            print("✅ Video producer started")
            
            # Stop producer after specified duration
            def stop_producer():
                time.sleep(duration)
                if process.poll() is None:
                    process.terminate()
                    print("⏹️ Video producer stopped after duration")
            
            threading.Thread(target=stop_producer, daemon=True).start()
            
        except Exception as e:
            print(f"❌ Failed to start video producer: {e}")
    
    def start_video_consumer(self):
        """Start the video frame consumer"""
        print("👁️ Starting video frame consumer...")
        try:
            process = subprocess.Popen([
                sys.executable, 'video_frame_consumer.py', '--mode', 'analyze'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.processes.append(('Video Consumer', process))
            print("✅ Video consumer started")
        except Exception as e:
            print(f"❌ Failed to start video consumer: {e}")
    
    def monitor_processes(self):
        """Monitor all running processes"""
        print("\n📊 Video Processing Monitor")
        print("=" * 50)
        
        while self.running:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Process Status:")
            for name, process in self.processes:
                if process.poll() is None:
                    print(f"  ✅ {name}: Running (PID: {process.pid})")
                else:
                    print(f"  ❌ {name}: Stopped (Exit code: {process.returncode})")
            
            time.sleep(5)
    
    def stop_all_processes(self):
        """Stop all running processes"""
        print("\n🛑 Stopping all video processing...")
        self.running = False
        
        for name, process in self.processes:
            if process.poll() is None:
                print(f"Stopping {name}...")
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    print(f"Force killed {name}")
        
        print("✅ All video processes stopped")
    
    def run_video_demo(self, duration=120, source='sample'):
        """Run the complete video processing demo"""
        print("🎥 Video Processing Demo")
        print("=" * 30)
        print(f"Duration: {duration} seconds")
        print(f"Source: {source}")
        print("This demo will show:")
        print("1. Video frame generation/capture")
        print("2. Real-time video stream processing with Flink")
        print("3. Video frame analysis and consumption")
        print("\nPress Ctrl+C to stop the demo early")
        
        # Set up signal handler for graceful shutdown
        def signal_handler(sig, frame):
            print("\n🛑 Video demo interrupted by user")
            self.stop_all_processes()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            # Start Flink job first
            self.start_video_flink_job()
            time.sleep(10)  # Give Flink time to start
            
            # Start consumer
            self.start_video_consumer()
            time.sleep(5)   # Give consumer time to start
            
            # Start producer
            self.start_video_producer(duration=duration, source=source)
            
            # Monitor processes
            monitor_thread = threading.Thread(target=self.monitor_processes, daemon=True)
            monitor_thread.start()
            
            # Wait for demo duration
            print(f"\n⏱️ Video demo running for {duration} seconds...")
            time.sleep(duration)
            
            print("\n✅ Video demo completed successfully!")
            
        except Exception as e:
            print(f"❌ Video demo error: {e}")
        finally:
            self.stop_all_processes()

def check_video_services():
    """Check if required services are running"""
    print("🔍 Checking video processing services...")
    
    try:
        # Check if Kafka is running
        result = subprocess.run([
            'docker', 'exec', 'kafka', 'kafka-topics', 
            '--bootstrap-server', 'localhost:9092', '--list'
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ Kafka is running")
            topics = result.stdout.strip().split('\n')
            video_topics = [t for t in topics if 'video' in t.lower()]
            print(f"   Video topics: {', '.join(video_topics) if video_topics else 'None'}")
        else:
            print("❌ Kafka is not running")
            return False
            
    except Exception as e:
        print(f"❌ Error checking Kafka: {e}")
        return False
    
    try:
        # Check if Flink is running
        import requests
        response = requests.get('http://localhost:8081', timeout=5)
        if response.status_code == 200:
            print("✅ Flink dashboard is accessible")
        else:
            print("❌ Flink dashboard is not accessible")
            return False
    except Exception as e:
        print(f"❌ Error checking Flink: {e}")
        return False
    
    return True

def main():
    """Main video demo function"""
    print("🎬 Video Processing Demo")
    print("=" * 25)
    
    # Check if services are running
    if not check_video_services():
        print("\n❌ Required services are not running.")
        print("Please run './setup.sh' first to start Kafka and Flink services.")
        return
    
    # Check if virtual environment is activated
    if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("\n⚠️  Virtual environment not detected.")
        print("Please run 'source venv/bin/activate' first.")
        return
    
    # Get demo parameters from user
    try:
        duration = input("\nEnter demo duration in seconds (default 120): ").strip()
        duration = int(duration) if duration else 120
        
        print("\nVideo source options:")
        print("1. sample - Generate sample frames (recommended)")
        print("2. webcam - Capture from webcam (requires camera)")
        print("3. video - Process video file (requires video file)")
        
        source_choice = input("Enter source choice (1-3, default 1): ").strip()
        
        if source_choice == '2':
            source = 'webcam'
        elif source_choice == '3':
            video_path = input("Enter path to video file: ").strip()
            if not os.path.exists(video_path):
                print(f"Error: Video file {video_path} does not exist")
                return
            source = 'video'
            # Note: video source would need additional handling
        else:
            source = 'sample'
            
    except ValueError:
        duration = 120
        source = 'sample'
    
    # Run the demo
    demo = VideoProcessingDemo()
    demo.run_video_demo(duration, source)

if __name__ == "__main__":
    main()
