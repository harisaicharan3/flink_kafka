#!/usr/bin/env python3
"""
Flink Stream Processing Job
Demonstrates basic Flink operations with Kafka integration
"""

import json
from datetime import datetime, timedelta
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

class TransactionData:
    """Data class for transaction records"""
    def __init__(self, transaction_id, user_id, product, category, amount, quantity, 
                 payment_method, timestamp, location):
        self.transaction_id = transaction_id
        self.user_id = user_id
        self.product = product
        self.category = category
        self.amount = amount
        self.quantity = quantity
        self.payment_method = payment_method
        self.timestamp = timestamp
        self.location = location
    
    def to_dict(self):
        return {
            'transaction_id': self.transaction_id,
            'user_id': self.user_id,
            'product': self.product,
            'category': self.category,
            'amount': self.amount,
            'quantity': self.quantity,
            'payment_method': self.payment_method,
            'timestamp': self.timestamp,
            'location': self.location
        }

class JsonToTransactionMap(MapFunction):
    """Map function to convert JSON string to TransactionData object"""
    def map(self, value):
        try:
            data = json.loads(value)
            return TransactionData(
                transaction_id=data['transaction_id'],
                user_id=data['user_id'],
                product=data['product'],
                category=data['category'],
                amount=float(data['amount']),
                quantity=int(data['quantity']),
                payment_method=data['payment_method'],
                timestamp=data['timestamp'],
                location=data['location']
            )
        except Exception as e:
            print(f"Error parsing transaction data: {e}")
            return None

class HighValueFilter(FilterFunction):
    """Filter function to keep only high-value transactions (> $100)"""
    def filter(self, value):
        if value is None:
            return False
        return value.amount > 100.0

class CategoryEnrichmentMap(MapFunction):
    """Map function to add category-based discounts"""
    def map(self, value):
        if value is None:
            return None
        
        # Apply category-based discounts
        discount_multiplier = 1.0
        if value.category == 'electronics':
            discount_multiplier = 0.95  # 5% discount
        elif value.category == 'books':
            discount_multiplier = 0.90  # 10% discount
        elif value.category == 'clothing':
            discount_multiplier = 0.92  # 8% discount
        
        # Create enriched transaction
        enriched_data = value.to_dict()
        enriched_data['original_amount'] = value.amount
        enriched_data['discount_applied'] = discount_multiplier
        enriched_data['final_amount'] = round(value.amount * discount_multiplier, 2)
        enriched_data['processing_timestamp'] = datetime.now().isoformat()
        
        return enriched_data

class TransactionToJsonMap(MapFunction):
    """Map function to convert transaction back to JSON string"""
    def map(self, value):
        if value is None:
            return "{}"
        return json.dumps(value)

class UserTransactionAggregator(KeyedProcessFunction):
    """Keyed process function to aggregate transactions by user"""
    def __init__(self):
        self.user_totals = {}
    
    def process_element(self, value, ctx):
        if value is None:
            return
        
        user_id = value['user_id']
        amount = value['final_amount']
        
        if user_id not in self.user_totals:
            self.user_totals[user_id] = {
                'user_id': user_id,
                'total_spent': 0.0,
                'transaction_count': 0,
                'last_updated': datetime.now().isoformat()
            }
        
        self.user_totals[user_id]['total_spent'] += amount
        self.user_totals[user_id]['transaction_count'] += 1
        self.user_totals[user_id]['last_updated'] = datetime.now().isoformat()
        
        # Output the updated user summary
        return json.dumps(self.user_totals[user_id])

def create_flink_job():
    """Create and configure the Flink streaming job"""
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism for demo purposes
    
    # Add Kafka connector JARs (these would be downloaded automatically in a real setup)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.18.0.jar")
    
    # Define Kafka consumer properties
    kafka_consumer_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    
    # Define Kafka producer properties
    kafka_producer_props = {
        'bootstrap.servers': 'localhost:9092'
    }
    
    # Create Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='transactions',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_consumer_props
    )
    
    # Create Kafka producer for processed data
    kafka_producer = FlinkKafkaProducer(
        topic='processed_transactions',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_producer_props
    )
    
    # Create Kafka producer for user aggregations
    user_aggregation_producer = FlinkKafkaProducer(
        topic='user_aggregations',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_producer_props
    )
    
    # Main processing pipeline
    print("Setting up Flink processing pipeline...")
    
    # 1. Read from Kafka
    transaction_stream = env.add_source(kafka_consumer)
    
    # 2. Parse JSON to TransactionData objects
    parsed_stream = transaction_stream.map(JsonToTransactionMap())
    
    # 3. Filter high-value transactions
    high_value_stream = parsed_stream.filter(HighValueFilter())
    
    # 4. Apply category-based enrichment
    enriched_stream = high_value_stream.map(CategoryEnrichmentMap())
    
    # 5. Convert back to JSON and write to processed_transactions topic
    json_stream = enriched_stream.map(TransactionToJsonMap())
    json_stream.add_sink(kafka_producer)
    
    # 6. Create user aggregation stream
    user_stream = enriched_stream.key_by(lambda x: x['user_id']) \
        .process(UserTransactionAggregator())
    
    # 7. Write user aggregations to separate topic
    user_stream.add_sink(user_aggregation_producer)
    
    # 8. Windowed aggregation example - total sales by category every 30 seconds
    category_stream = enriched_stream.key_by(lambda x: x['category']) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .reduce(lambda a, b: {
            'category': a['category'],
            'total_amount': a.get('total_amount', a['final_amount']) + b['final_amount'],
            'transaction_count': a.get('transaction_count', 1) + 1,
            'window_end': datetime.now().isoformat()
        })
    
    # Convert category aggregations to JSON and print (for demo)
    category_json_stream = category_stream.map(lambda x: json.dumps(x))
    category_json_stream.print("Category Aggregations")
    
    return env

def main():
    """Main function to run the Flink job"""
    print("Starting Flink Kafka Processing Job")
    print("==================================")
    
    try:
        # Create and execute the Flink job
        env = create_flink_job()
        
        print("Job configuration complete. Starting execution...")
        print("The job will process transactions from the 'transactions' topic")
        print("and write results to 'processed_transactions' and 'user_aggregations' topics")
        print("Press Ctrl+C to stop the job")
        
        # Execute the job
        env.execute("Kafka-Flink Processing Job")
        
    except Exception as e:
        print(f"Error running Flink job: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
