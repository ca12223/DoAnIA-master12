#!/usr/bin/env python3
"""
MQTT Topic Enumeration Attack Script (Windows Compatible)
=========================================================
Simulates topic enumeration attacks to discover available topics and test ACL security.
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

class TopicEnumerationAttack:
    def __init__(self, broker_host="localhost", broker_port=1883, 
                 username=None, password=None, use_tls=False):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.attack_stats = {
            "topics_tested": 0,
            "topics_discovered": 0,
            "subscriptions_successful": 0,
            "subscriptions_failed": 0,
            "connections_failed": 0,
            "start_time": None,
            "end_time": None
        }
        self.discovered_topics = set()
        
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
    
    def generate_topic_candidates(self):
        """Generate comprehensive list of topic candidates for enumeration"""
        candidates = []
        
        # Common IoT topic patterns
        tenants = ["tenantA", "tenantB", "admin", "system", "test", "demo"]
        zones = ["factory", "office", "storage", "security", "energy", "production"]
        devices = ["Temperature", "Humidity", "Motion", "CO-Gas", "Smoke", "Light", "Door", "Fan"]
        actions = ["telemetry", "command", "status", "config", "alarm", "log"]
        
        # Single level topics
        single_level = ["system", "admin", "config", "status", "health", "monitor", "debug", "test"]
        candidates.extend(single_level)
        
        # Two level topics
        for zone in zones:
            candidates.append(zone)
            for tenant in tenants:
                candidates.append(f"{zone}/{tenant}")
        
        # Three level topics
        for zone in zones:
            for tenant in tenants:
                for device in devices:
                    candidates.append(f"{zone}/{tenant}/{device}")
        
        # Four level topics
        for zone in zones:
            for tenant in tenants:
                for device in devices:
                    for action in actions:
                        candidates.append(f"{zone}/{tenant}/{device}/{action}")
        
        # System topics
        system_topics = [
            "system/status",
            "system/config",
            "system/logs",
            "system/health",
            "system/metrics",
            "system/alerts",
            "system/users",
            "system/acl",
            "system/security",
            "system/performance"
        ]
        candidates.extend(system_topics)
        
        # Admin topics
        admin_topics = [
            "admin/config",
            "admin/users",
            "admin/acl",
            "admin/logs",
            "admin/security",
            "admin/system",
            "admin/database",
            "admin/network",
            "admin/backup",
            "admin/restore"
        ]
        candidates.extend(admin_topics)
        
        # Security topics
        security_topics = [
            "security/alerts",
            "security/events",
            "security/logs",
            "security/auth",
            "security/acl",
            "security/audit",
            "security/monitor",
            "security/blocked",
            "security/failed",
            "security/success"
        ]
        candidates.extend(security_topics)
        
        # Config topics
        config_topics = [
            "config/database",
            "config/network",
            "config/security",
            "config/users",
            "config/acl",
            "config/system",
            "config/performance",
            "config/logging",
            "config/monitoring",
            "config/backup"
        ]
        candidates.extend(config_topics)
        
        # Debug topics
        debug_topics = [
            "debug/system",
            "debug/network",
            "debug/security",
            "debug/performance",
            "debug/logs",
            "debug/internal",
            "debug/trace",
            "debug/memory",
            "debug/cpu",
            "debug/disk"
        ]
        candidates.extend(debug_topics)
        
        # Random topics
        for i in range(100):
            random_topic = f"random/{random.randint(1,100)}/{random.randint(1,100)}/{random.randint(1,100)}"
            candidates.append(random_topic)
        
        # Numeric topics
        for i in range(1, 51):
            candidates.append(f"topic{i}")
            candidates.append(f"device{i}")
            candidates.append(f"sensor{i}")
            candidates.append(f"node{i}")
        
        return list(set(candidates))  # Remove duplicates
    
    def enumeration_worker(self, worker_id, topic_candidates, delay_ms=200):
        """Worker thread for topic enumeration"""
        client_id = f"topic_enumerator_{worker_id}"
        client = self.create_client(client_id)
        
        if not client:
            self.attack_stats["connections_failed"] += 1
            return
        
        def on_connect(client, userdata, flags, rc, properties):
            if rc == 0:
                print(f" Worker {worker_id}: Connected, starting topic enumeration...")
                
                # Try to subscribe to each topic candidate
                for topic in topic_candidates:
                    try:
                        result = client.subscribe(topic, qos=0)
                        self.attack_stats["topics_tested"] += 1
                        
                        if result[0] == 0:
                            self.attack_stats["subscriptions_successful"] += 1
                            self.discovered_topics.add(topic)
                            print(f" Worker {worker_id}: Discovered topic: {topic}")
                        else:
                            self.attack_stats["subscriptions_failed"] += 1
                        
                        # Delay between attempts
                        if delay_ms > 0:
                            time.sleep(delay_ms / 1000.0)
                            
                    except Exception as e:
                        self.attack_stats["subscriptions_failed"] += 1
                        if self.attack_stats["topics_tested"] % 50 == 0:  # Don't spam errors
                            print(f" Worker {worker_id}: Error testing topic {topic}: {e}")
            else:
                print(f" Worker {worker_id}: Connection failed: {rc}")
                self.attack_stats["connections_failed"] += 1
        
        def on_message(client, userdata, msg, properties):
            self.attack_stats["topics_discovered"] += 1
            if self.attack_stats["topics_discovered"] % 10 == 0:  # Don't spam
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
            
            # Wait for enumeration to complete
            time.sleep(len(topic_candidates) * delay_ms / 1000.0 + 10)
            
            print(f" Worker {worker_id}: Completed topic enumeration")
            
        except Exception as e:
            print(f" Worker {worker_id}: Error: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
    
    def launch_attack(self, num_workers=2, delay_ms=200, custom_topics=None):
        """Launch topic enumeration attack"""
        if custom_topics:
            topic_candidates = custom_topics
        else:
            topic_candidates = self.generate_topic_candidates()
        
        print(f" Starting Topic Enumeration Attack")
        print(f"   Workers: {num_workers}")
        print(f"   Topic candidates: {len(topic_candidates)}")
        print(f"   Delay between attempts: {delay_ms}ms")
        print("=" * 60)
        
        self.attack_stats["start_time"] = time.time()
        
        # Create worker threads
        threads = []
        for i in range(num_workers):
            thread = threading.Thread(
                target=self.enumeration_worker,
                args=(i, topic_candidates, delay_ms)
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
        print(f"Topics tested: {self.attack_stats['topics_tested']}")
        print(f"Topics discovered: {len(self.discovered_topics)}")
        print(f"Subscriptions successful: {self.attack_stats['subscriptions_successful']}")
        print(f"Subscriptions failed: {self.attack_stats['subscriptions_failed']}")
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        
        if self.attack_stats['topics_tested'] > 0:
            success_rate = (self.attack_stats['subscriptions_successful'] / 
                           self.attack_stats['topics_tested'] * 100)
            print(f"Discovery success rate: {success_rate:.1f}%")
        
        if duration > 0:
            print(f"Topics tested per second: {self.attack_stats['topics_tested']/duration:.1f}")
        
        # Print discovered topics
        if self.discovered_topics:
            print(f"\n Discovered Topics ({len(self.discovered_topics)}):")
            print("-" * 40)
            for topic in sorted(self.discovered_topics):
                print(f"   {topic}")

def main():
    parser = argparse.ArgumentParser(description="MQTT Topic Enumeration Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--tls", action="store_true", help="Use TLS connection")
    parser.add_argument("--workers", type=int, default=2, help="Number of worker threads")
    parser.add_argument("--delay", type=int, default=200, help="Delay between attempts (ms)")
    parser.add_argument("--topics", nargs="+", help="Custom topic candidates")
    
    args = parser.parse_args()
    
    # Create attack instance
    attack = TopicEnumerationAttack(
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
