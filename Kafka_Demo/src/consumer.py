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

Kafka Consumer Demo
Demonstrates various consumer configurations, including:
- Consumer groups
- Offset management
- Rebalancing
- Consumption patterns
"""

import os
import sys
import json
import time
import signal # useful for shutdown management
import logging
import argparse
from uuid import uuid4
from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("kafka-consumer")

# get all environment variables from the .env file
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPICS = {
    "ORDERS": "orders",
    "NOTIFICATIONS": "notifications", 
    "LOGS": "logs"
}

# Global flag for handling shutdown, similar to while True: ...
running = True


def handle_signal(sig, frame):
    """
    Call back function for crtl+C/kill signal
    termination handling
    """
    global running
    logger.info(f"Received signal {sig}, shutting down...")
    running = False

def print_assignment(consumer, partitions):
    """
    Another callback function
    for partitions are assigned
    """
    logger.info(f"Assigned partitions: {partitions}")
    try:
        for partition in partitions:
            # Get stored offset for each assigned partition
            # a method from Confluent Kafka module.
            # Takes a list of topic partitions and returns the current position (offset) for each partition
            position = consumer.position([partition]) 
            if position: 
                logger.info(f"Current position for {partition} is {position[0].offset}")
    except Exception as e:
        logger.error(f"Error in partition assignment callback: {e}")

def print_revocation(consumer, partitions):
    """
    Callback for when partitions are revoked
    If we need to rebalance, then partitions will be revoked first then reassigned
    """
    logger.info(f"Revoked partitions: {partitions}")

def create_consumer(group_id=None, auto_offset_reset='earliest', 
                   enable_auto_commit=True, auto_commit_interval_ms=5000,
                   session_timeout_ms=30000, max_poll_interval_ms=300000,
                   fetch_max_bytes=52428800, max_partition_fetch_bytes=1048576):
    """
    Create a Kafka consumer with specified configuration parameters
    
    Args:
        group_id: Consumer group ID (None if do not belong to any groups)
        auto_offset_reset: Where to start consuming if no offset info (i.e. if a consumer is started for the 1st time)
        enable_auto_commit: Whether to automatically commit offsets
        auto_commit_interval_ms: How often to auto-commit offsets
        session_timeout_ms: How long before a consumer is considered dead
        max_poll_interval_ms: Maximum time between poll() calls
        fetch_max_bytes: Maximum bytes to fetch per request
        max_partition_fetch_bytes: Maximum bytes to fetch per partition

        Do not memorize all these config params. I look them up myself as well.

    Returns:
        A configured Kafka consumer
    """
    # If no group_id specified, generate some random consuemer group
    # Partions are assigned to standalone consumer manually.
    if group_id is None:
        group_id = f"standalone-consumer-{uuid4()}"
    
    # instead of hard coing, you can also put everything in a Config file, just like .env file
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'client.id': f"{group_id}-{uuid4()}",
        'auto.offset.reset': auto_offset_reset,
        'enable.auto.commit': enable_auto_commit,
        'auto.commit.interval.ms': auto_commit_interval_ms,
        'session.timeout.ms': session_timeout_ms,
        'max.poll.interval.ms': max_poll_interval_ms,
        'fetch.max.bytes': fetch_max_bytes,
        'max.partition.fetch.bytes': max_partition_fetch_bytes
    }
    
    consumer = Consumer(config)
    
    # Note: We'll subscribe in the specific functions rather than here
    # since we don't have the topic information at this point
    
    return consumer

def reset_offsets(consumer, topic, position='beginning'):
    """
    Reset consumer offsets for a topic
    
    Args:
        consumer: Kafka consumer instance
        topic: Topic name
        position: Either 'beginning' or 'end'
    """
    # Get the current assignment. There are just way too many methods for different purposes
    # Official documentation is always your best friend
    assignment = consumer.assignment()
    if not assignment:
        logger.warning("No partitions assigned yet, subscribing to a topic first")
        consumer.subscribe([topic])
        time.sleep(2)  # Wait for assignment
        assignment = consumer.assignment()
    
    # Set offsets based on position
    if position == 'beginning':
        for partition in assignment:
            if partition.topic == topic:
                partition.offset = OFFSET_BEGINNING
    elif position == 'end':
        for partition in assignment:
            if partition.topic == topic:
                partition.offset = OFFSET_END
    else:
        raise ValueError("Position must be 'beginning' or 'end'") #since we are resetting. And we expect some error like this to happen so we raise an exception
    
    # Seek to the specified offsets
    consumer.assign(assignment)

def seek_to_timestamp(consumer, topic, timestamp_ms):
    """
    Jumps to the messages around a specific timestamp
    
    Args:
        consumer: Kafka consumer instance
        topic: Topic name
        timestamp_ms: Timestamp in milliseconds since epoch
    """
    # Get the current assignment
    assignment = consumer.assignment()
    if not assignment:
        logger.warning("No partitions assigned yet, subscribing first")
        consumer.subscribe([topic])
        time.sleep(2)  # Wait for assignment
        assignment = consumer.assignment()
    
    # Filter only partitions for the specified topic
    topic_partitions = [p for p in assignment if p.topic == topic]
    
    # Get offsets for the timestamp
    offsets = consumer.offsets_for_times([(p.topic, p.partition, timestamp_ms) for p in topic_partitions])
    
    # Seek to the returned offsets
    for offset in offsets:
        if offset.offset != -1:  # -1 means no offset for this timestamp
            logger.info(f"Seeking {offset.topic}:{offset.partition} to offset {offset.offset}")
            consumer.seek(offset)

def demonstrate_consumer_groups(topic, group_id):
    """
    Demonstrate how consumer groups work
    
    This consumer will join a consumer group and receive a portion of the partitions.
    """
    logger.info(f"=== Demonstrating Consumer Groups with '{group_id}' ===")
    
    consumer = create_consumer(
        group_id=group_id,
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    try:
        # Subscribe to the topic with callbacks
        logger.info(f"Subscribing to '{topic}' in group '{group_id}'")
        consumer.subscribe([topic], on_assign=print_assignment, on_revoke=print_revocation)
        
        # Consume messages until interrupted
        msg_count = 0
        while running and msg_count < 50:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition {msg.partition()}") # We catch errors very explicitly
                else:
                    logger.error(f"Consumer error: {msg.error()}")
            else:
                # Process the message
                topic = msg.topic()
                partition = msg.partition()
                offset = msg.offset()
                key = msg.key().decode('utf-8') if msg.key() else None
                
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"[Group: {group_id}] Received message: "
                              f"topic={topic}, partition={partition}, offset={offset}, key={key}")
                    
                    # Print order details if it's an order
                    # Prob not the best way to determine new order or update order. We shall have a falg for that

                    if 'order_id' in value:
                        if 'status' in value and 'total_amount' not in value:
                            # This is an order update
                            logger.info(f"Order {value['order_id']} updated to status: {value['status']}")
                        else:
                            # This is a new order
                            logger.info(f"New order {value['order_id']} received with amount: {value.get('total_amount')}")
                    
                    msg_count += 1
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse message value as JSON: {msg.value()}")
                
        logger.info(f"Consumer in group '{group_id}' processed {msg_count} messages")
    finally:
        # Close the consumer to leave the group cleanly
        consumer.close()

def demonstrate_manual_offset_management(topic):
    """
    We can mannually assign a partition as well
    """
    logger.info("=== Demonstrating Manual Offset Management ===")
    
    # Create consumer with auto-commit disabled
    consumer = create_consumer(
        group_id=f"manual-commit-{uuid4()}",
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )
    
    try:
        # Subscribe to the topic with callbacks
        logger.info(f"Subscribing to '{topic}' with manual commits")
        consumer.subscribe([topic], on_assign=print_assignment, on_revoke=print_revocation)
        
        # Consume messages until interrupted
        msg_count = 0
        last_commit = time.time()
        
        while running and msg_count < 30: # 30 is arbitrary fyi
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition {msg.partition()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
            else:
                # Process the message
                topic = msg.topic()
                partition = msg.partition()
                offset = msg.offset()
                
                logger.info(f"[Manual commit] Received: topic={topic}, partition={partition}, offset={offset}")
                
                # Simulate processing
                time.sleep(0.1)
                
                # Commit every 10 messages or 5 seconds
                current_time = time.time()
                if msg_count % 10 == 0 or (current_time - last_commit) > 5:
                    try:
                        # Synchronous commit
                        consumer.commit(asynchronous=False)
                        logger.info(f"Manually committed offsets after {msg_count} messages")
                        last_commit = current_time
                    except KafkaException as e:
                        logger.error(f"Failed to commit offsets: {e}")
                
                msg_count += 1
                
        # Final commit before closing
        if msg_count > 0:
            try:
                consumer.commit(asynchronous=False)
                logger.info(f"Final manual commit after {msg_count} messages")
            except KafkaException as e:
                logger.error(f"Failed to commit offsets: {e}")
                
        logger.info(f"Manual offset consumer processed {msg_count} messages")
    finally:
        consumer.close()

def demonstrate_consumption_patterns(topic):
    """
    Demonstrate different consumption patterns
    Recall that during lecture I mentioned we can send msg one by one, but can also send in batch for reducing network costs
    
    """
    logger.info("=== Demonstrating Consumption Patterns ===")
    
    # 1. Batch processing pattern
    logger.info("1. Batch Processing Pattern")
    batch_consumer = create_consumer(
        group_id=f"batch-consumer-{uuid4()}",
        auto_offset_reset='earliest',
        enable_auto_commit=False  # Manual commit for batch control
    )
    
    try:
        # Subscribe to topic
        logger.info(f"Subscribing to '{topic}' for BATCH processing demo")
        # using the callback functions we defined previously
        batch_consumer.subscribe([topic], on_assign=print_assignment, on_revoke=print_revocation) # again, you can find all these config params on the official doc
        
        # Process messages in batches
        batch_size = 10 #arbitrary
        batch = []
        
        logger.info(f"Collecting a batch of {batch_size} messages...")
        
        # Collect a batch of messages
        while running and len(batch) < batch_size:
            msg = batch_consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    break
            else:
                batch.append(msg)
        
        # Process the entire batch at once
        if batch:
            logger.info(f"Processing batch of {len(batch)} messages")
            for msg in batch:
                logger.info(f"Batch message: topic={msg.topic()}, "
                          f"partition={msg.partition()}, offset={msg.offset()}")
            
            # Commit after processing the entire batch
            batch_consumer.commit(asynchronous=False)
            logger.info("Committed offsets after batch processing")
    finally:
        batch_consumer.close()
    
    # 2. Continuous streaming pattern
    logger.info("2. Continuous Streaming Pattern (simulated)")
    stream_consumer = create_consumer(
        group_id=f"stream-consumer-{uuid4()}",
        auto_offset_reset='latest',  # Start from latest to simulate streaming
        enable_auto_commit=True,
        auto_commit_interval_ms=1000  # Frequent auto-commits for 1000ms
    )
    
    try:
        # Subscribe to topic
        logger.info(f"Subscribing to '{topic}' for stream processing")
        stream_consumer.subscribe([topic], on_assign=print_assignment, on_revoke=print_revocation) 
        
        # Simulate streaming for a short period
        # tbh I doubt if you can see a major difference. The data volume is low and modern computers should be cabable of handling everything.
        # It'd be critical if tillions of msg
        start_time = time.time() 
        msg_count = 0
        
        logger.info("Starting stream processing...")
        
        while running and (time.time() - start_time) < 10:  # Process for 10 seconds
            msg = stream_consumer.poll(timeout=0.1)  # Short timeout for quick polling
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    break
            else:
                # Process message immediately
                logger.info(f"Stream message: topic={msg.topic()}, "
                          f"partition={msg.partition()}, offset={msg.offset()}")
                msg_count += 1
        
        logger.info(f"Processed {msg_count} messages in streaming mode")
    finally:
        stream_consumer.close()
    
    logger.info("Consumption patterns demo completed")

def demonstrate_seek_operations(topic):
    """
    Demonstrate seek operations

    """
    logger.info("=== Demonstrating Seek Operations ===")
    
    # Create consumer with a unique group
    consumer = create_consumer(
        group_id=f"seek-demo-{uuid4()}",
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    
    try:
        # Subscribe to the topic with callbacks
        logger.info(f"Subscribing to '{topic}'")
        consumer.subscribe([topic], on_assign=print_assignment, on_revoke=print_revocation)
        
        # 1. Seek to beginning
        logger.info("1. Seeking to beginning of all partitions")
        reset_offsets(consumer, topic, position='beginning')
        
        # Consume a few messages
        msg_count = 0
        while running and msg_count < 5:
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                logger.info(f"[Beginning] Received: topic={msg.topic()}, "
                          f"partition={msg.partition()}, offset={msg.offset()}")
                msg_count += 1
        
        # 2. Seek to end
        logger.info("2. Seeking to end of all partitions")
        reset_offsets(consumer, topic, position='end')
        
        # Try to consume (should be no messages unless new ones arrive)
        logger.info("Polling at end position (should timeout unless new messages arrive)...")
        msg = consumer.poll(timeout=3.0)
        if msg and not msg.error():
            logger.info(f"Received new message: partition={msg.partition()}, offset={msg.offset()}")
        
        # 3. Seek to specific timestamp (e.g., 10 minutes ago)
        timestamp_ms = int((time.time() - 600) * 1000)  # 10 minutes ago
        logger.info(f"3. Seeking to timestamp: {timestamp_ms} (10 minutes ago)")
        seek_to_timestamp(consumer, topic, timestamp_ms)
        
        # Consume messages from this timestamp
        msg_count = 0
        while running and msg_count < 5:
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                logger.info(f"[Timestamp] Received: topic={msg.topic()}, "
                          f"partition={msg.partition()}, offset={msg.offset()}")
                msg_count += 1
        
        logger.info("Seek operations demo completed")
    finally:
        consumer.close()

def main():
    """Main function to run the consumer demonstrations"""
    # Register signal handlers for shutdown
    # It's more elegant than pressing CRTL+C or kill the process in your task manager
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    # Parse command-line arguments
    # Preset flags for different executions
    parser = argparse.ArgumentParser(description='Kafka Consumer Demo')
    parser.add_argument('--demo', type=str, choices=['all', 'group', 'offset', 'seek', 'patterns'],
                       default='all', help='Which demonstration to run')
    parser.add_argument('--topic', type=str, default=TOPICS["ORDERS"],
                       help='Topic to consume from')
    parser.add_argument('--group', type=str, default=f"demo-group-{uuid4()}",
                       help='Consumer group ID base name')
    args = parser.parse_args()
    
    try:
        # Run the selected demonstrations
        if args.demo in ['all', 'group']:
            # Run two consumers in the same group to demonstrate partition assignment
            # I use threading for extra fancy
            from threading import Thread
            
            # Define a function for the thread
            def run_consumer(instance_id):
                group_id = f"{args.group}-{instance_id}"
                demonstrate_consumer_groups(args.topic, group_id)
            
            # Create and start threads
            threads = []
            for i in range(2):
                t = Thread(target=run_consumer, args=(i,))
                t.daemon = True
                threads.append(t)
                t.start()
            
            # Wait for threads to complete (or timeout)
            for t in threads:
                t.join(timeout=30)
        
        if args.demo in ['all', 'offset']:
            demonstrate_manual_offset_management(args.topic)
        
        if args.demo in ['all', 'seek']:
            demonstrate_seek_operations(args.topic)
        
        if args.demo in ['all', 'patterns']:
            demonstrate_consumption_patterns(args.topic)
        
        logger.info("Consumer demo completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Consumer terminated by user")
        return 0
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())