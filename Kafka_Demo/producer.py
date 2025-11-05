#!/usr/bin/env python3
"""
Copyright (C) 2025 BeaconFire Staffing Solutions
Author: Ray Wang (ray.wang@beaconfireinc.com)

This file is part of Feb DE Batch Kafka Project demo.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

Kafka Producer Demo
Demonstrates:
1, Configuration options (acks, retries, compression)
2, Callback handling
3, Message delivery strategies
4, Transactional semantics
"""

import os
import sys
import json
import time
import random
import logging # a better alternative to print(). Contains loggers, handlers, formatters and severity levels
from datetime import datetime
from uuid import uuid4 #to generate random uid
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

# Configure logging
# logger: The main entry point for the logging system. You create a logger, and then use it to emit log messages.
logging.basicConfig(level=logging.INFO, #min severity level INFO
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') # can only use formated string. Using a f-string will throw errors
                
logger = logging.getLogger("kafka-producer")

# Configs, refer to your Docker image for dir, port info, etc... 
# We should define all the topics beforehand and avoid topic auto creation
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPICS = {
    "ORDERS": "orders",
    "NOTIFICATIONS": "notifications",
    "LOGS": "logs"
}
PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Kitchen", "Beauty"]
ORDER_STATUSES = ["CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELED"]

# Initialize faker for generating sample data
fake = Faker()

# define callbacks
# functions that are executed after a specific event occurs, such as when a message is successfully delivered or when an error occurs.
def delivery_callback(err, msg):
    """Callback executed when a message is successfully delivered or fails"""
    if err:
        logger.error(f'Message delivery failed: {err}')
    else:
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        key = msg.key().decode('utf-8') if msg.key() else None
        logger.info(f'Message delivered to {topic}-{partition} at offset {offset} with key={key}')

def create_producer(acks='all', retries=5, delivery_timeout_ms=30000, 
                    linger_ms=5, batch_size=16384, compression_type='snappy', 
                    idempotence=True):
    """Create a Kafka producer with specified configuration parameters
    
    Args:
        acks: Acknowledgment level ('0', '1', or 'all')
        retries: Number of retries before giving up
        delivery_timeout_ms: Max time to wait for message delivery
        linger_ms: Time to wait for additional messages before sending a BATCH (sending in batches rather than individual msgs reduces network costs)
        batch_size: Maximum size of message batches
        compression_type: Type of compression ('none', 'gzip', 'snappy', 'lz4'). I honestly do not know and do not give a f*** about different compression types 
        idempotence: message delever only once. (The producer assigns each message a unique sequence number and producer ID. 
                    The broker uses these to detect and prevent duplicate messages that might occur due to producer retries.)
    
    Returns:
        A configured Kafka producer
    """
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': f'python-producer-{uuid4()}',
        'acks': acks,
        'retries': retries,
        'delivery.timeout.ms': delivery_timeout_ms,
        'linger.ms': linger_ms,
        'batch.size': batch_size,
        'compression.type': compression_type,
    }
    
    if idempotence:
        # Recall that idempotence for exactly-once semantics
        config.update({
            'enable.idempotence': True,
            'max.in.flight.requests.per.connection': 5,
            'acks': 'all',  # Must be 'all' for idempotence. Think about why?
        })
    
    return Producer(config)

def generate_order():
    """
    Author: Ray Wang 02/25/2025 
    Generate a sample order

    Args: None
    Return: order_id, customer_id, and json like order data
    """
    order_id = str(uuid4())
    customer_id = random.randint(1000, 9999)
    
    order = {
        'order_id': order_id,
        'customer_id': customer_id,
        'timestamp': datetime.now().isoformat(),
        'status': ORDER_STATUSES[0],  # Always start with CREATED
        'items': [],
        'total_amount': 0.0
    }
    
    # Add 1-5 items to the order, ofc can add much more for fun
    num_items = random.randint(1, 5)
    for _ in range(num_items):
        price = round(random.uniform(10.0, 500.0), 2)
        quantity = random.randint(1, 3)
        category = random.choice(PRODUCT_CATEGORIES)
        
        item = {
            'product_id': str(uuid4())[:8],
            'product_name': fake.word().title(),
            'category': category,
            'price': price,
            'quantity': quantity,
            'subtotal': round(price * quantity, 2)
        }
        
        order['items'].append(item)
        order['total_amount'] += item['subtotal']
    # 2 decimal places to display good
    order['total_amount'] = round(order['total_amount'], 2)
    
    return order_id, customer_id, order

def generate_order_update(order_id, customer_id):
    """
    Generate an update for a randonly picked existing order
    """
    # pick one
    current_status_idx = random.randint(1, len(ORDER_STATUSES) - 1)
    
    update = {
        'order_id': order_id,
        'customer_id': customer_id,
        'timestamp': datetime.now().isoformat(),
        'status': ORDER_STATUSES[current_status_idx],
    }
    
    return update

def send_with_partitioning_strategies(producer, num_messages=10):
    """
    Demonstrate different partitioning strategies
    See below for more detailed explainations 
    """
    logger.info("=== Demonstrating Partitioning Strategies ===")
    
    # 1. Default Partitioning (default when no key is provided)
    # Previously Round Robin -> Sticky Session
    logger.info("1. Default Partitioning (no keys)")
    for _ in range(num_messages):
        order_id, customer_id, order = generate_order()
        producer.produce(
            topic=TOPICS["ORDERS"],
            key=None,
            value=json.dumps(order).encode('utf-8'),
            callback=delivery_callback
        )
        producer.poll(0)  # Trigger delivery reports
    
    # 2. Key-based Partitioning (consistent hashing)
    logger.info("2. Key-based Partitioning (using customer_id as key)")
    for _ in range(num_messages):
        order_id, customer_id, order = generate_order()
        # Here we use customer_id as key to ensure: 
        # all orders from the same customergo to the same partition
        # important for the correct maintaining ordering
        producer.produce(
            topic=TOPICS["ORDERS"],
            key=str(customer_id).encode('utf-8'),
            value=json.dumps(order).encode('utf-8'),
            callback=delivery_callback
        )
        producer.poll(0)

    # 3. Explicit Partition Selection
    logger.info("3. Explicit Partition Selection")
    for _ in range(num_messages):
        order_id, customer_id, order = generate_order()
        # Explicitly choose partition 0
        producer.produce(
            topic=TOPICS["ORDERS"],
            key=str(customer_id).encode('utf-8'),
            value=json.dumps(order).encode('utf-8'),
            partition=0,
            callback=delivery_callback
        )
        producer.poll(0)
    
    # Ensure all messages are sent
    producer.flush()

def demonstrate_delivery_semantics(num_messages=5):
    """
    Demonstrate different message delivery guarantee semantics
    """
    logger.info("=== Demonstrating Delivery Semantics ===")
    
    # 1. At-most-once semantics (acks=0)
    logger.info("1. At-most-once delivery (acks=0, fire and forget)")
    at_most_once_producer = create_producer(acks='0', idempotence=False)
    for _ in range(num_messages):
        order_id, customer_id, order = generate_order()
        at_most_once_producer.produce(
            topic=TOPICS["LOGS"],  # Use logs topic for less critical data
            key=order_id.encode('utf-8'),
            value=json.dumps(order).encode('utf-8'),
            # No callback - we don't care about delivery confirmation
        )
        at_most_once_producer.poll(0)
    at_most_once_producer.flush()
    
    # 2. At-least-once semantics (acks=all, retries>0)
    logger.info("2. At-least-once delivery (acks=all, retries enabled)")
    at_least_once_producer = create_producer(acks='all', retries=5, idempotence=False)
    for _ in range(num_messages):
        order_id, customer_id, order = generate_order()
        at_least_once_producer.produce(
            topic=TOPICS["NOTIFICATIONS"],
            key=order_id.encode('utf-8'),
            value=json.dumps(order).encode('utf-8'),
            callback=delivery_callback  # We want to know if delivery succeeds
        )
        at_least_once_producer.poll(0)
    at_least_once_producer.flush()
    
    # 3. Exactly-once semantics (idempotence=True)
    logger.info("3. Exactly-once delivery (idempotence enabled)")
    exactly_once_producer = create_producer(idempotence=True)
    
    # Create a set of orders that we'll both create and update
    # to demonstrate transactional semantics
    orders = []
    for _ in range(num_messages):
        order_id, customer_id, order = generate_order()
        orders.append((order_id, customer_id, order))
        exactly_once_producer.produce(
            topic=TOPICS["ORDERS"],
            key=order_id.encode('utf-8'),
            value=json.dumps(order).encode('utf-8'),
            callback=delivery_callback
        )
        exactly_once_producer.poll(0)
    
    # Wait a bit then send updates to the same orders
    time.sleep(1)
    for order_id, customer_id, _ in orders:
        update = generate_order_update(order_id, customer_id)
        exactly_once_producer.produce(
            topic=TOPICS["ORDERS"],
            key=order_id.encode('utf-8'),
            value=json.dumps(update).encode('utf-8'),
            callback=delivery_callback
        )
        exactly_once_producer.poll(0)
    
    exactly_once_producer.flush()

def demonstrate_batching_compression():
    """
    Demonstrate batching and compression configurations
    Recall that we dont always need to send msg one by one
    Can pack messages together to send in batches
    """
    logger.info("=== Demonstrating Batching and Compression ===")
    
    # Create producer with larger batch size and longer linger time
    batch_producer = create_producer(
        linger_ms=100,  # Wait 100ms to collect more messages
        batch_size=64000,  # Larger batch size
        compression_type='gzip'  # Use gzip compression
    )
    
    # Send a batch of messages quickly
    logger.info("Sending 100 messages with batching and compression")
    for _ in range(100):
        order_id, customer_id, order = generate_order()
        batch_producer.produce(
            topic=TOPICS["LOGS"],
            key=order_id.encode('utf-8'),
            value=json.dumps(order).encode('utf-8'),
            callback=delivery_callback
        )
        # Don't poll between sends to allow batching
    
    # Now flush to send any remaining messages
    start_time = time.time()
    batch_producer.flush()
    end_time = time.time()
    logger.info(f"Batch sent in {end_time - start_time:.3f} seconds")

def main():
    """
    Main function to run the producer demonstrations
    Comment out functions you do not want to use
    """
    try:
        # Create our a producer
        producer = create_producer()
        
        # partitioning strategies demo
        send_with_partitioning_strategies(producer)
        
        # different delivery semantics demo
        demonstrate_delivery_semantics()
        
        # batching and compression demo
        demonstrate_batching_compression()
        
        logger.info("Producer demo completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Producer terminated by user")
        return 0
    except Exception as e:
        logger.error(f"Producer error: {e}")
        return 1
    
if __name__ == "__main__":
    sys.exit(main())
