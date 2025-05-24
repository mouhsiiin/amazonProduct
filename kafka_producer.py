#!/usr/bin/env python3
"""
Kafka Producer for Amazon Product Reviews
Reads review data from Data.json and streams it to Kafka topic
"""

import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import argparse
from typing import Dict, Any
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AmazonReviewProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='amazon-reviews'):
        """
        Initialize Kafka producer for Amazon reviews
        
        Args:
            bootstrap_servers (str): Kafka bootstrap servers
            topic (str): Kafka topic name
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        
    def connect(self):
        """Establish connection to Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432,
                compression_type='gzip'
            )
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def send_review(self, review_data: Dict[Any, Any]) -> bool:
        """
        Send a single review to Kafka topic
        
        Args:
            review_data (dict): Review data to send
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Use reviewerID + asin as key for partitioning
            key = f"{review_data.get('reviewerID', 'unknown')}_{review_data.get('asin', 'unknown')}"
            
            future = self.producer.send(
                self.topic,
                key=key,
                value=review_data
            )
            
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            logger.debug(f"Sent review to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send review: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending review: {e}")
            return False
    
    def load_and_stream_reviews(self, data_file='Data.json', delay_ms=100, max_records=None):
        """
        Load reviews from JSON file and stream to Kafka
        
        Args:
            data_file (str): Path to the data file
            delay_ms (int): Delay between messages in milliseconds
            max_records (int): Maximum number of records to send (None for all)
        """
        if not os.path.exists(data_file):
            logger.error(f"Data file {data_file} not found")
            return False
            
        try:
            sent_count = 0
            failed_count = 0
            
            logger.info(f"Starting to stream reviews from {data_file}")
            
            with open(data_file, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    if max_records and sent_count >= max_records:
                        break
                        
                    try:
                        # Parse JSON line
                        review = json.loads(line.strip())
                        
                        # Add metadata
                        review['_timestamp'] = int(time.time())
                        review['_line_number'] = line_num
                        
                        # Send to Kafka
                        if self.send_review(review):
                            sent_count += 1
                            if sent_count % 1000 == 0:
                                logger.info(f"Sent {sent_count} reviews")
                        else:
                            failed_count += 1
                            
                        # Add delay between messages
                        if delay_ms > 0:
                            time.sleep(delay_ms / 1000.0)
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON on line {line_num}: {e}")
                        failed_count += 1
                        continue
                    except Exception as e:
                        logger.error(f"Error processing line {line_num}: {e}")
                        failed_count += 1
                        continue
            
            # Flush remaining messages
            self.producer.flush()
            
            logger.info(f"Streaming completed. Sent: {sent_count}, Failed: {failed_count}")
            return True
            
        except Exception as e:
            logger.error(f"Error streaming reviews: {e}")
            return False
    
    def close(self):
        """Close the producer connection"""
        if self.producer:
            self.producer.close()
            logger.info("Producer connection closed")

def main():
    parser = argparse.ArgumentParser(description='Amazon Reviews Kafka Producer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', 
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='amazon-reviews',
                       help='Kafka topic name (default: amazon-reviews)')
    parser.add_argument('--data-file', default='Data.json',
                       help='Path to data file (default: Data.json)')
    parser.add_argument('--delay', type=int, default=100,
                       help='Delay between messages in milliseconds (default: 100)')
    parser.add_argument('--max-records', type=int, default=None,
                       help='Maximum number of records to send (default: all)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create producer
    producer = AmazonReviewProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    try:
        # Connect to Kafka
        if not producer.connect():
            logger.error("Failed to connect to Kafka. Exiting.")
            sys.exit(1)
        
        # Stream reviews
        success = producer.load_and_stream_reviews(
            data_file=args.data_file,
            delay_ms=args.delay,
            max_records=args.max_records
        )
        
        if success:
            logger.info("Producer completed successfully")
        else:
            logger.error("Producer failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        producer.close()

if __name__ == "__main__":
    main()