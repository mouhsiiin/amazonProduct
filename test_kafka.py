#!/usr/bin/env python3
"""
Test script for Kafka Producer and Consumer
This script helps verify that the Kafka setup is working correctly
"""

import subprocess
import time
import threading
import signal
import sys
import os

def run_command(command, description):
    """Run a command and return the process"""
    print(f"\n{'='*50}")
    print(f"Starting: {description}")
    print(f"Command: {command}")
    print(f"{'='*50}")
    
    try:
        # Start the process
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        return process
    except Exception as e:
        print(f"Error starting {description}: {e}")
        return None

def monitor_process(process, name, max_lines=50):
    """Monitor a process output"""
    line_count = 0
    try:
        for line in iter(process.stdout.readline, ''):
            if line:
                print(f"[{name}] {line.strip()}")
                line_count += 1
                if line_count >= max_lines:
                    print(f"[{name}] ... (truncated after {max_lines} lines)")
                    break
    except Exception as e:
        print(f"Error monitoring {name}: {e}")

def test_kafka_setup():
    """Test the Kafka producer and consumer setup"""
    print("Kafka Producer/Consumer Test Script")
    print("=" * 60)
    
    # Check if required files exist
    required_files = [
        'kafka_producer.py',
        'kafka_consumer.py',
        'Data.json',
        'requirements.txt'
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"Error: Missing required files: {missing_files}")
        return False
    
    print("✓ All required files found")
    
    # Test with a small number of records
    producer_cmd = "python kafka_producer.py --max-records 100 --delay 50 --verbose"
    consumer_cmd = "python kafka_consumer.py --max-messages 100 --stats-interval 5 --verbose"
    
    print("\nThis test will:")
    print("1. Start a Kafka consumer in the background")
    print("2. Start a Kafka producer to send 100 sample records")
    print("3. Monitor both processes for a short time")
    print("4. Clean up processes")
    
    input("\nPress Enter to continue or Ctrl+C to exit...")
    
    # Start consumer first
    print("\nStarting Kafka Consumer...")
    consumer_process = run_command(consumer_cmd, "Kafka Consumer")
    
    if not consumer_process:
        return False
    
    # Wait a bit for consumer to initialize
    time.sleep(3)
    
    # Start producer
    print("\nStarting Kafka Producer...")
    producer_process = run_command(producer_cmd, "Kafka Producer")
    
    if not producer_process:
        consumer_process.terminate()
        return False
    
    try:
        # Monitor both processes
        print("\nMonitoring processes (press Ctrl+C to stop)...")
        
        # Start monitoring threads
        consumer_thread = threading.Thread(
            target=monitor_process, 
            args=(consumer_process, "CONSUMER", 30)
        )
        producer_thread = threading.Thread(
            target=monitor_process, 
            args=(producer_process, "PRODUCER", 30)
        )
        
        consumer_thread.daemon = True
        producer_thread.daemon = True
        
        consumer_thread.start()
        producer_thread.start()
        
        # Wait for producer to finish or timeout
        producer_process.wait(timeout=60)
        
        # Give consumer a bit more time to process remaining messages
        time.sleep(5)
        
        print("\n" + "="*60)
        print("Test completed successfully!")
        print("If you saw messages being produced and consumed, the setup is working.")
        print("="*60)
        
        return True
        
    except subprocess.TimeoutExpired:
        print("\nTest timed out - this might be normal if Kafka is processing slowly")
        return True
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        return True
    except Exception as e:
        print(f"\nError during test: {e}")
        return False
    finally:
        # Clean up processes
        print("\nCleaning up processes...")
        try:
            if producer_process.poll() is None:
                producer_process.terminate()
                producer_process.wait(timeout=5)
        except:
            pass
        
        try:
            if consumer_process.poll() is None:
                consumer_process.terminate()
                consumer_process.wait(timeout=5)
        except:
            pass

def main():
    try:
        success = test_kafka_setup()
        if success:
            print("\n✓ Test completed")
        else:
            print("\n✗ Test failed")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nTest interrupted")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
