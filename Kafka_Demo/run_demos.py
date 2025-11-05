#!/usr/bin/env python3
"""
Script to run the Kafka producer and consumer demos
"""

import os
import sys
import time
import logging
import argparse
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("kafka-demo-runner")

def run_demo(demo_type, wait_time=30):
    """
    Run a specific demo script. It's an entry point to run these scripts more consistently
    ofc you can do "python3 producer.py" for sure. 
    
    Args:
        demo_type: Type of demo to run ('producer', 'consumer', or 'both')
        wait_time: Time to wait between starting producer and consumer
    """
    try:
        if demo_type in ['producer', 'both']:
            logger.info("=== Starting Producer Demo ===")
            producer_proc = subprocess.Popen(['python', 'producer.py'])
            
            # If we're only running the producer, wait for it to complete
            if demo_type == 'producer':
                producer_proc.wait()
                if producer_proc.returncode == 0: # exit code 0 -> success. Everything else indicates an error
                    logger.info("Producer demo completed successfully")
                else:
                    logger.error(f"Producer demo failed with exit code {producer_proc.returncode}")
                return producer_proc.returncode
            
            # If we're running both, wait before starting the consumer
            logger.info(f"Waiting {wait_time} seconds for messages to be produced...")
            time.sleep(wait_time) #by default 30. Change the default value however you like, or provide a new one when calling this func
        
        if demo_type in ['consumer', 'both']:
            logger.info("=== Starting Consumer Demo ===")
            consumer_proc = subprocess.Popen(['python', 'consumer.py'])
            consumer_result = consumer_proc.wait()
            
            if consumer_result == 0:
                logger.info("Consumer demo completed successfully")
            else:
                logger.error(f"Consumer demo failed with exit code {consumer_result}")
        
        # If we're running both, wait for the producer to complete
        if demo_type == 'both':
            producer_result = producer_proc.wait()
            if producer_result == 0:
                logger.info("Producer demo completed successfully")
            else:
                logger.error(f"Producer demo failed with exit code {producer_result}")
            
            return max(producer_result, consumer_result)
        
        # If we only ran the consumer, return its result
        if demo_type == 'consumer':
            return consumer_result
    
    except KeyboardInterrupt: # when you press ctrl+c
        logger.info("Demo terminated by user")
        if 'producer_proc' in locals() and producer_proc.poll() is None:
            producer_proc.terminate()
        if 'consumer_proc' in locals() and consumer_proc.poll() is None:
            consumer_proc.terminate()
        return 130  # standard exit code for SIGINT
    
    except Exception as e:
        logger.error(f"Error running demo: {e}")
        return 1 # or any code other than 0

def run_specific_demo(demo_name):
    """
    Run a specific producer or consumer demonstration
    
    Args:
        demo_name: Name of the specific demo to run
    """
    try:
        if demo_name.startswith('producer:'):
            # Extract the producer demo name
            producer_demo = demo_name.split(':', 1)[1]
            
            # Import the producer module to access its functions
            # tbh the best practice is to put module files in an independent folder.
            import producer
            
            # Get the corresponding function
            if producer_demo == 'partitioning':
                logger.info("=== Running Producer Partitioning Demo ===")
                producer.send_with_partitioning_strategies(producer.create_producer())
            elif producer_demo == 'delivery':
                logger.info("=== Running Producer Delivery Semantics Demo ===")
                producer.demonstrate_delivery_semantics()
            elif producer_demo == 'batching':
                logger.info("=== Running Producer Batching Demo ===")
                producer.demonstrate_batching_compression()
            else:
                logger.error(f"Unknown producer demo: {producer_demo}")
                return 1
        
        elif demo_name.startswith('consumer:'):
            # Extract the consumer demo name
            consumer_demo = demo_name.split(':', 1)[1]
            
            # Run the consumer with the appropriate --demo flag
            cmd = ['python', 'consumer.py', '--demo', consumer_demo]
            logger.info(f"Running command: {' '.join(cmd)}")
            
            process = subprocess.Popen(cmd)
            return process.wait()
        
        else:
            logger.error(f"Unknown demo: {demo_name}")
            return 1
        
        return 0
    
    except Exception as e:
        logger.error(f"Error running specific demo: {e}")
        return 1

def main():
    """
    parse arguments and run demos
    """
    parser = argparse.ArgumentParser(description='Run Kafka Producer and Consumer Demos')
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--type', type=str, choices=['producer', 'consumer', 'both'],
                      help='Type of demo to run')
    group.add_argument('--demo', type=str,
                      help='Specific demo to run (e.g., producer:partitioning, consumer:offset)')
    
    parser.add_argument('--wait', type=int, default=30,
                       help='Seconds to wait between starting producer and consumer')
    
    args = parser.parse_args()
    
    if args.demo:
        return run_specific_demo(args.demo)
    else:
        return run_demo(args.type, args.wait)

if __name__ == "__main__":
    sys.exit(main())
