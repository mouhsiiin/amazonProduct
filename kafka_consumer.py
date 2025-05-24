#!/usr/bin/env python3
"""
Kafka Consumer for Amazon Product Reviews
Consumes review data from Kafka topic and processes it
"""

import json
import logging
import signal
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import argparse
from typing import Dict, Any, List
import time
import threading
from collections import defaultdict, Counter
import statistics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AmazonReviewConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='amazon-reviews', 
                 group_id='amazon-review-consumer-group'):
        """
        Initialize Kafka consumer for Amazon reviews
        
        Args:
            bootstrap_servers (str): Kafka bootstrap servers
            topic (str): Kafka topic name
            group_id (str): Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.running = False
        
        # Statistics tracking
        self.stats = {
            'total_messages': 0,
            'messages_per_second': 0,
            'rating_distribution': Counter(),
            'product_counts': Counter(),
            'reviewer_counts': Counter(),
            'start_time': None,
            'last_stats_time': time.time()
        }
        
        # Thread-safe statistics updates
        self.stats_lock = threading.Lock()
        
    def connect(self):
        """Establish connection to Kafka"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                auto_offset_reset='earliest',  # Start from beginning if no committed offset
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                session_timeout_ms=30000,
                max_poll_records=1000,
                consumer_timeout_ms=1000  # Timeout for polling
            )
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
            logger.info(f"Subscribed to topic '{self.topic}' with group ID '{self.group_id}'")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def process_review(self, review_data: Dict[Any, Any]) -> bool:
        """
        Process a single review message
        
        Args:
            review_data (dict): Review data from Kafka
            
        Returns:
            bool: True if processed successfully, False otherwise
        """
        try:
            # Extract key fields
            reviewer_id = review_data.get('reviewerID', 'unknown')
            asin = review_data.get('asin', 'unknown')
            overall_rating = review_data.get('overall', 0)
            review_text = review_data.get('reviewText', '')
            summary = review_data.get('summary', '')
            helpful = review_data.get('helpful', [0, 0])
            
            # Update statistics
            with self.stats_lock:
                self.stats['total_messages'] += 1
                self.stats['rating_distribution'][overall_rating] += 1
                self.stats['product_counts'][asin] += 1
                self.stats['reviewer_counts'][reviewer_id] += 1
            
            # Log every 1000th message or high-rated reviews
            if (self.stats['total_messages'] % 1000 == 0 or 
                overall_rating == 5.0 and len(review_text) > 100):
                logger.info(f"Processed review #{self.stats['total_messages']}: "
                           f"ASIN={asin}, Rating={overall_rating}, "
                           f"Reviewer={reviewer_id[:10]}...")
            
            # Example processing: identify highly helpful reviews
            if len(helpful) >= 2 and helpful[1] > 0:
                helpfulness_ratio = helpful[0] / helpful[1]
                if helpfulness_ratio > 0.8 and helpful[1] >= 5:
                    logger.info(f"Highly helpful review found: {helpfulness_ratio:.2f} "
                               f"({helpful[0]}/{helpful[1]}) for product {asin}")
            
            # Example processing: detect potentially fake reviews
            if (overall_rating == 5.0 and 
                len(review_text) < 50 and 
                'great' in review_text.lower() and 
                'perfect' in review_text.lower()):
                logger.warning(f"Potentially suspicious review detected for product {asin}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing review: {e}")
            return False
    
    def print_statistics(self):
        """Print current processing statistics"""
        with self.stats_lock:
            current_time = time.time()
            time_elapsed = current_time - self.stats['start_time'] if self.stats['start_time'] else 0
            
            if time_elapsed > 0:
                self.stats['messages_per_second'] = self.stats['total_messages'] / time_elapsed
            
            print("\n" + "="*60)
            print("KAFKA CONSUMER STATISTICS")
            print("="*60)
            print(f"Total Messages Processed: {self.stats['total_messages']:,}")
            print(f"Messages per Second: {self.stats['messages_per_second']:.2f}")
            print(f"Runtime: {time_elapsed:.1f} seconds")
            
            # Rating distribution
            print("\nRating Distribution:")
            for rating in sorted(self.stats['rating_distribution'].keys()):
                count = self.stats['rating_distribution'][rating]
                percentage = (count / self.stats['total_messages'] * 100) if self.stats['total_messages'] > 0 else 0
                print(f"  {rating} stars: {count:,} ({percentage:.1f}%)")
            
            # Top products
            print("\nTop 10 Products by Review Count:")
            for asin, count in self.stats['product_counts'].most_common(10):
                print(f"  {asin}: {count:,} reviews")
            
            # Top reviewers
            print("\nTop 10 Most Active Reviewers:")
            for reviewer, count in self.stats['reviewer_counts'].most_common(10):
                print(f"  {reviewer[:15]}...: {count:,} reviews")
            
            print("="*60 + "\n")
    
    def consume_messages(self, max_messages=None, print_stats_interval=10):
        """
        Start consuming messages from Kafka topic
        
        Args:
            max_messages (int): Maximum number of messages to consume (None for unlimited)
            print_stats_interval (int): Interval in seconds to print statistics
        """
        self.running = True
        self.stats['start_time'] = time.time()
        processed_count = 0
        failed_count = 0
        last_stats_print = time.time()
        
        logger.info("Starting message consumption...")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                if max_messages and processed_count >= max_messages:
                    logger.info(f"Reached maximum message limit: {max_messages}")
                    break
                
                try:
                    # Process the message
                    success = self.process_review(message.value)
                    
                    if success:
                        processed_count += 1
                    else:
                        failed_count += 1
                    
                    # Print statistics periodically
                    current_time = time.time()
                    if current_time - last_stats_print >= print_stats_interval:
                        self.print_statistics()
                        last_stats_print = current_time
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    failed_count += 1
                    continue
        
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Error during consumption: {e}")
        finally:
            logger.info(f"Consumption ended. Processed: {processed_count}, Failed: {failed_count}")
            self.print_statistics()
    
    def stop(self):
        """Stop the consumer"""
        self.running = False
        logger.info("Consumer stop requested")
    
    def close(self):
        """Close the consumer connection"""
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer connection closed")

def signal_handler(signum, frame, consumer):
    """Handle graceful shutdown on SIGINT/SIGTERM"""
    logger.info("Received shutdown signal")
    consumer.stop()

def main():
    parser = argparse.ArgumentParser(description='Amazon Reviews Kafka Consumer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='amazon-reviews',
                       help='Kafka topic name (default: amazon-reviews)')
    parser.add_argument('--group-id', default='amazon-review-consumer-group',
                       help='Consumer group ID (default: amazon-review-consumer-group)')
    parser.add_argument('--max-messages', type=int, default=None,
                       help='Maximum number of messages to consume (default: unlimited)')
    parser.add_argument('--stats-interval', type=int, default=10,
                       help='Statistics print interval in seconds (default: 10)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create consumer
    consumer = AmazonReviewConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id
    )
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, consumer))
    signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, consumer))
    
    try:
        # Connect to Kafka
        if not consumer.connect():
            logger.error("Failed to connect to Kafka. Exiting.")
            sys.exit(1)
        
        # Start consuming
        consumer.consume_messages(
            max_messages=args.max_messages,
            print_stats_interval=args.stats_interval
        )
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()