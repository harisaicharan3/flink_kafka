#!/usr/bin/env python3
"""
Demo script that runs the complete Flink-Kafka pipeline
Shows the end-to-end data flow with timing and statistics
"""

import time
import threading
import subprocess
import sys
import signal
import os
from datetime import datetime

class FlinkKafkaDemo:
    def __init__(self):
        self.processes = []
        self.running = True
        
    def start_flink_job(self):
        """Start the Flink processing job"""
        print("🚀 Starting Flink processing job...")
        try:
            process = subprocess.Popen([
                sys.executable, 'flink_processor.py'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.processes.append(('Flink Job', process))
            print("✅ Flink job started")
        except Exception as e:
            print(f"❌ Failed to start Flink job: {e}")
    
    def start_producer(self, duration=60):
        """Start the Kafka producer"""
        print(f"📊 Starting data producer for {duration} seconds...")
        try:
            process = subprocess.Popen([
                sys.executable, 'kafka_producer.py'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.processes.append(('Producer', process))
            print("✅ Producer started")
            
            # Stop producer after specified duration
            def stop_producer():
                time.sleep(duration)
                if process.poll() is None:
                    process.terminate()
                    print("⏹️ Producer stopped after duration")
            
            threading.Thread(target=stop_producer, daemon=True).start()
            
        except Exception as e:
            print(f"❌ Failed to start producer: {e}")
    
    def start_consumer(self):
        """Start the Kafka consumer"""
        print("👀 Starting data consumer...")
        try:
            process = subprocess.Popen([
                sys.executable, 'kafka_consumer.py'
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.processes.append(('Consumer', process))
            print("✅ Consumer started")
        except Exception as e:
            print(f"❌ Failed to start consumer: {e}")
    
    def monitor_processes(self):
        """Monitor all running processes"""
        print("\n📈 Process Monitor")
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
        print("\n🛑 Stopping all processes...")
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
        
        print("✅ All processes stopped")
    
    def run_demo(self, duration=120):
        """Run the complete demo"""
        print("🎬 Flink-Kafka Learning Demo")
        print("=" * 40)
        print(f"Duration: {duration} seconds")
        print("This demo will show:")
        print("1. Real-time data generation")
        print("2. Stream processing with Flink")
        print("3. Data consumption and monitoring")
        print("\nPress Ctrl+C to stop the demo early")
        
        # Set up signal handler for graceful shutdown
        def signal_handler(sig, frame):
            print("\n🛑 Demo interrupted by user")
            self.stop_all_processes()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            # Start Flink job first
            self.start_flink_job()
            time.sleep(10)  # Give Flink time to start
            
            # Start consumer
            self.start_consumer()
            time.sleep(5)   # Give consumer time to start
            
            # Start producer
            self.start_producer(duration=duration)
            
            # Monitor processes
            monitor_thread = threading.Thread(target=self.monitor_processes, daemon=True)
            monitor_thread.start()
            
            # Wait for demo duration
            print(f"\n⏱️ Demo running for {duration} seconds...")
            time.sleep(duration)
            
            print("\n✅ Demo completed successfully!")
            
        except Exception as e:
            print(f"❌ Demo error: {e}")
        finally:
            self.stop_all_processes()

def check_services():
    """Check if required services are running"""
    print("🔍 Checking services...")
    
    try:
        # Check if Kafka is running
        result = subprocess.run([
            'docker', 'exec', 'kafka', 'kafka-topics', 
            '--bootstrap-server', 'localhost:9092', '--list'
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ Kafka is running")
            topics = result.stdout.strip().split('\n')
            print(f"   Topics: {', '.join(topics) if topics else 'None'}")
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
    """Main demo function"""
    print("🎯 Flink-Kafka Learning Demo")
    print("=" * 30)
    
    # Check if services are running
    if not check_services():
        print("\n❌ Required services are not running.")
        print("Please run './setup.sh' first to start Kafka and Flink services.")
        return
    
    # Check if virtual environment is activated
    if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("\n⚠️  Virtual environment not detected.")
        print("Please run 'source venv/bin/activate' first.")
        return
    
    # Get demo duration from user
    try:
        duration = input("\nEnter demo duration in seconds (default 120): ").strip()
        duration = int(duration) if duration else 120
    except ValueError:
        duration = 120
    
    # Run the demo
    demo = FlinkKafkaDemo()
    demo.run_demo(duration)

if __name__ == "__main__":
    main()
