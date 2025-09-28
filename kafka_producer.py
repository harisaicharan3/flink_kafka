#!/usr/bin/env python3
"""
Kafka Producer - Generates sample data for Flink processing
Demonstrates basic Kafka producer operations
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

class SampleDataProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        
    def generate_sample_data(self):
        """Generate sample e-commerce transaction data"""
        products = ['laptop', 'phone', 'tablet', 'headphones', 'camera', 'book', 'shirt', 'shoes']
        categories = ['electronics', 'clothing', 'books', 'accessories']
        payment_methods = ['credit_card', 'debit_card', 'paypal', 'cash']
        
        return {
            'transaction_id': f"txn_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            'user_id': f"user_{random.randint(1, 1000)}",
            'product': random.choice(products),
            'category': random.choice(categories),
            'amount': round(random.uniform(10.0, 1000.0), 2),
            'quantity': random.randint(1, 5),
            'payment_method': random.choice(payment_methods),
            'timestamp': datetime.now().isoformat(),
            'location': f"city_{random.randint(1, 50)}"
        }
    
    def send_to_topic(self, topic_name, data, key=None):
        """Send data to Kafka topic"""
        try:
            future = self.producer.send(topic_name, value=data, key=key)
            record_metadata = future.get(timeout=10)
            print(f"Sent data to topic: {topic_name}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
            return True
        except KafkaError as e:
            print(f"Failed to send data: {e}")
            return False
    
    def produce_continuous_data(self, topic_name, interval=2, count=100):
        """Continuously produce sample data"""
        print(f"Starting to produce {count} messages to topic '{topic_name}'...")
        
        for i in range(count):
            data = self.generate_sample_data()
            key = data['user_id']  # Use user_id as key for partitioning
            
            success = self.send_to_topic(topic_name, data, key)
            if success:
                print(f"Message {i+1}/{count}: {data['transaction_id']} - ${data['amount']}")
            
            time.sleep(interval)
        
        print("Finished producing data")
    
    def close(self):
        """Close the producer"""
        self.producer.close()

def main():
    producer = SampleDataProducer()
    
    try:
        # Create topics (you might need to create these manually or via Kafka admin)
        topics = ['transactions', 'processed_transactions', 'high_value_transactions']
        
        print("Kafka Producer Demo")
        print("==================")
        
        # Produce sample transaction data
        print("\n1. Producing transaction data...")
        producer.produce_continuous_data('transactions', interval=1, count=50)
        
        # Produce some high-value transactions
        print("\n2. Producing high-value transactions...")
        for i in range(10):
            data = producer.generate_sample_data()
            data['amount'] = round(random.uniform(500.0, 2000.0), 2)  # High value
            producer.send_to_topic('high_value_transactions', data, data['user_id'])
            print(f"High-value transaction: {data['transaction_id']} - ${data['amount']}")
            time.sleep(0.5)
        
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
