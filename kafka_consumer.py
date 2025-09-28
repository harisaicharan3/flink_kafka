#!/usr/bin/env python3
"""
Kafka Consumer - Demonstrates basic Kafka consumer operations
Shows how to consume from multiple topics and process messages
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

class MultiTopicConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}
    
    def create_consumer(self, topic_name, group_id=None):
        """Create a consumer for a specific topic"""
        if group_id is None:
            group_id = f"consumer-group-{topic_name}"
        
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            consumer_timeout_ms=1000  # Timeout after 1 second of no messages
        )
        
        self.consumers[topic_name] = consumer
        return consumer
    
    def consume_messages(self, topic_name, max_messages=10, timeout=30):
        """Consume messages from a specific topic"""
        if topic_name not in self.consumers:
            self.create_consumer(topic_name)
        
        consumer = self.consumers[topic_name]
        print(f"\nConsuming messages from topic '{topic_name}'...")
        print("=" * 50)
        
        message_count = 0
        start_time = time.time()
        
        try:
            for message in consumer:
                if time.time() - start_time > timeout:
                    print(f"Timeout reached ({timeout}s), stopping consumption")
                    break
                
                if message_count >= max_messages:
                    print(f"Reached maximum messages ({max_messages}), stopping consumption")
                    break
                
                message_count += 1
                print(f"Message {message_count}:")
                print(f"  Topic: {message.topic}")
                print(f"  Partition: {message.partition}")
                print(f"  Offset: {message.offset}")
                print(f"  Key: {message.key}")
                print(f"  Timestamp: {datetime.fromtimestamp(message.timestamp/1000) if message.timestamp else 'N/A'}")
                print(f"  Value: {json.dumps(message.value, indent=2)}")
                print("-" * 30)
                
        except Exception as e:
            print(f"Error consuming messages: {e}")
        
        print(f"Consumed {message_count} messages from '{topic_name}'")
        return message_count
    
    def consume_continuous(self, topic_name, duration=60):
        """Continuously consume messages for a specified duration"""
        if topic_name not in self.consumers:
            self.create_consumer(topic_name)
        
        consumer = self.consumers[topic_name]
        print(f"\nContinuously consuming from topic '{topic_name}' for {duration} seconds...")
        print("=" * 50)
        
        start_time = time.time()
        message_count = 0
        
        try:
            for message in consumer:
                if time.time() - start_time > duration:
                    print(f"Duration reached ({duration}s), stopping consumption")
                    break
                
                message_count += 1
                current_time = datetime.now().strftime("%H:%M:%S")
                
                # Display different information based on topic
                if topic_name == 'processed_transactions':
                    data = message.value
                    print(f"[{current_time}] Processed Transaction: {data.get('transaction_id', 'N/A')} - "
                          f"${data.get('final_amount', 0):.2f} (was ${data.get('original_amount', 0):.2f})")
                elif topic_name == 'user_aggregations':
                    data = message.value
                    print(f"[{current_time}] User Summary: {data.get('user_id', 'N/A')} - "
                          f"Total: ${data.get('total_spent', 0):.2f} ({data.get('transaction_count', 0)} transactions)")
                else:
                    data = message.value
                    print(f"[{current_time}] {topic_name}: {data.get('transaction_id', 'N/A')} - "
                          f"${data.get('amount', 0):.2f}")
                
        except Exception as e:
            print(f"Error in continuous consumption: {e}")
        
        print(f"Consumed {message_count} messages in {duration} seconds")
        return message_count
    
    def close_all(self):
        """Close all consumers"""
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
                print(f"Closed consumer for topic: {topic}")
            except Exception as e:
                print(f"Error closing consumer for {topic}: {e}")

def demonstrate_consumer_operations():
    """Demonstrate various consumer operations"""
    consumer_manager = MultiTopicConsumer()
    
    try:
        print("Kafka Consumer Demo")
        print("==================")
        
        # 1. Consume from original transactions topic
        print("\n1. Consuming from 'transactions' topic...")
        consumer_manager.consume_messages('transactions', max_messages=5)
        
        # 2. Consume from processed transactions topic
        print("\n2. Consuming from 'processed_transactions' topic...")
        consumer_manager.consume_messages('processed_transactions', max_messages=5)
        
        # 3. Consume from user aggregations topic
        print("\n3. Consuming from 'user_aggregations' topic...")
        consumer_manager.consume_messages('user_aggregations', max_messages=5)
        
        # 4. Continuous consumption demo
        print("\n4. Continuous consumption demo (30 seconds)...")
        print("This will show real-time processing results")
        consumer_manager.consume_continuous('processed_transactions', duration=30)
        
    except KeyboardInterrupt:
        print("\nStopping consumer demo...")
    finally:
        consumer_manager.close_all()

def monitor_topics():
    """Monitor all topics for new messages"""
    consumer_manager = MultiTopicConsumer()
    
    topics = ['transactions', 'processed_transactions', 'user_aggregations', 'high_value_transactions']
    
    print("Multi-Topic Monitor")
    print("==================")
    print("Monitoring topics:", ", ".join(topics))
    print("Press Ctrl+C to stop monitoring")
    
    try:
        # Create consumers for all topics
        for topic in topics:
            consumer_manager.create_consumer(topic, group_id=f"monitor-group-{topic}")
        
        # Simple round-robin consumption
        while True:
            for topic in topics:
                consumer = consumer_manager.consumers[topic]
                try:
                    # Try to get one message with short timeout
                    messages = consumer.poll(timeout_ms=1000, max_records=1)
                    
                    for topic_partition, message_list in messages.items():
                        for message in message_list:
                            current_time = datetime.now().strftime("%H:%M:%S")
                            print(f"[{current_time}] {topic}: {message.value.get('transaction_id', 'N/A')} - "
                                  f"${message.value.get('amount', message.value.get('final_amount', 0)):.2f}")
                
                except Exception as e:
                    # Ignore timeout errors
                    pass
            
            time.sleep(0.1)  # Small delay to prevent excessive CPU usage
            
    except KeyboardInterrupt:
        print("\nStopping topic monitor...")
    finally:
        consumer_manager.close_all()

def main():
    """Main function with menu options"""
    print("Kafka Consumer Operations Demo")
    print("=============================")
    print("1. Demonstrate basic consumer operations")
    print("2. Monitor all topics continuously")
    print("3. Exit")
    
    while True:
        try:
            choice = input("\nEnter your choice (1-3): ").strip()
            
            if choice == '1':
                demonstrate_consumer_operations()
            elif choice == '2':
                monitor_topics()
            elif choice == '3':
                print("Goodbye!")
                break
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
                
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
