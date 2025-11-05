#!/usr/bin/env python3
"""
Kafka Transactional Producer Demo
Demonstrates exactly-once semantics with transactions.
"""

import os
import sys
import json
import time
import logging
import argparse
from uuid import uuid4
from confluent_kafka import Producer, KafkaError, KafkaException
from faker import Faker

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("kafka-transactional-producer")

# Constants
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPICS = {
    "ORDERS": "orders",
    "INVENTORY": "inventory",
    "SHIPMENTS": "shipments"
}

# Initialize faker for generating sample data
fake = Faker()

def create_transactional_producer(transactional_id=None):
    """Create a Kafka producer with transactional capabilities
    
    Args:
        transactional_id: Unique ID for the transactional producer
                         (must be consistent across restarts)
    
    Returns:
        A configured Kafka transactional producer
    """
    if transactional_id is None:
        transactional_id = f"tx-producer-{uuid4()}"
    
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': f"tx-client-{uuid4()}",
        'transactional.id': transactional_id,
        'enable.idempotence': True,
        'acks': 'all',
        'retries': 10,
        'max.in.flight.requests.per.connection': 5
    }
    
    producer = Producer(config)
    
    # Initialize transactions
    producer.init_transactions()
    
    return producer

def generate_order():
    """Generate a sample order"""
    order_id = str(uuid4())
    product_id = str(uuid4())[:8]
    quantity = random.randint(1, 5)
    
    order = {
        'order_id': order_id,
        'product_id': product_id,
        'quantity': quantity,
        'customer_name': fake.name(),
        'timestamp': int(time.time() * 1000)
    }
    
    return order, product_id, quantity

def generate_inventory_update(product_id, quantity):
    """Generate an inventory update (reduction)"""
    return {
        'product_id': product_id,
        'quantity_change': -quantity,  # Negative for reduction
        'timestamp': int(time.time() * 1000)
    }

def generate_shipment(order_id, product_id, quantity):
    """Generate a shipment record"""
    return {
        'shipment_id': str(uuid4()),
        'order_id': order_id,
        'product_id': product_id,
        'quantity': quantity,
        'status': 'PENDING',
        'shipping_address': fake.address(),
        'timestamp': int(time.time() * 1000)
    }

def demonstrate_successful_transaction(producer):
    """Demonstrate a successful transaction that updates multiple topics atomically"""
    logger.info("=== Demonstrating Successful Transaction ===")
    
    try:
        # Begin transaction
        producer.begin_transaction()
        logger.info("Transaction started")
        
        # 1. Create an order
        order, product_id, quantity = generate_order()
        order_id = order['order_id']
        
        logger.info(f"Produced order: {order_id} for {quantity} units of product {product_id}")
        producer.produce(
            topic=TOPICS["ORDERS"],
            key=order_id.encode('utf-8'),
            value=json.dumps(order).encode('utf-8')
        )
        
        # 2. Update inventory
        inventory_update = generate_inventory_update(product_id, quantity)
        logger.info(f"Updating inventory for product {product_id}: {inventory_update}")
        producer.produce(
            topic=TOPICS["INVENTORY"],
            key=product_id.encode('utf-8'),
            value=json.dumps(inventory_update).encode('utf-8')
        )
        
        # 3. Create shipment
        shipment = generate_shipment(order_id, product_id, quantity)
        logger.info(f"Creating shipment: {shipment['shipment_id']} for order {order_id}")
        producer.produce(
            topic=TOPICS["SHIPMENTS"],
            key=order_id.encode('utf-8'),
            value=json.dumps(shipment).encode('utf-8')
        )
        
        # Commit the transaction
        producer.commit_transaction()
        logger.info("Transaction committed successfully")
        
    except KafkaException as e:
        logger.error(f"Transaction failed: {e}")
        # Abort the transaction on error
        producer.abort_transaction()
        logger.info("Transaction aborted")

def demonstrate_aborted_transaction(producer):
    """Demonstrate an aborted transaction"""
    logger.info("=== Demonstrating Aborted Transaction ===")
    
    try:
        # Begin transaction
        producer.begin_transaction()
        logger.info("Transaction started")
        
        # 1. Create an order
        order, product_id, quantity = generate_order()
        order_id = order['order_id']
        
        logger.info(f"Produced order: {order_id} for {quantity} units of product {product_id}")
        producer.produce(
            topic=TOPICS["ORDERS"],
            key=order_id.encode('utf-8'),
            value=json.dumps(order).encode('utf-8')
        )
        
        # 2. Update inventory
        inventory_update = generate_inventory_update(product_id, quantity)
        logger.info(f"Updating inventory for product {product_id}: {inventory_update}")
        producer.produce(
            topic=TOPICS["INVENTORY"],
            key=product_id.encode('utf-8'),
            value=json.dumps(inventory_update).encode('utf-8')
        )
        
        # Simulate a business logic error
        logger.info("Simulating a business logic error (insufficient inventory)")
        raise ValueError("Insufficient inventory")
        
        # This code won't be reached due to the exception
        shipment = generate_shipment(order_id, product_id, quantity)
        producer.produce(
            topic=TOPICS["SHIPMENTS"],
            key=order_id.encode('utf-8'),
            value=json.dumps(shipment).encode('utf-8')
        )
        
        # Commit the transaction
        producer.commit_transaction()
        
    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
        producer.abort_transaction()
        logger.info("Transaction aborted due to Kafka error")
    except Exception as e:
        logger.error(f"Business logic error: {e}")
        # Abort the transaction on error
        producer.abort_transaction()
        logger.info("Transaction aborted due to business logic error")

def main():
    """Main function to run the transactional producer demonstrations"""
    # Import here to avoid circular import
    import random
    
    parser = argparse.ArgumentParser(description='Kafka Transactional Producer Demo')
    parser.add_argument('--demo', type=str, choices=['success', 'abort', 'both'],
                      default='both', help='Which demonstration to run')
    args = parser.parse_args()
    
    try:
        # Create our transactional producer
        transactional_id = f"demo-tx-{uuid4()}"
        producer = create_transactional_producer(transactional_id)
        
        # Run the requested demonstrations
        if args.demo in ['success', 'both']:
            demonstrate_successful_transaction(producer)
        
        if args.demo in ['abort', 'both']:
            demonstrate_aborted_transaction(producer)
        
        logger.info("Transactional producer demo completed!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Producer terminated by user")
        return 0
    except Exception as e:
        logger.error(f"Producer error: {e}")
        return 1
    
if __name__ == "__main__":
    sys.exit(main())
