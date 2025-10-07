#!/usr/bin/env python3
"""
MQTT Payload Anomaly Attack Script (Windows Compatible)
=======================================================
Simulates payload anomaly attacks to test EMQX payload validation and schema enforcement.
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

class PayloadAnomalyAttack:
    def __init__(self, broker_host="localhost", broker_port=1883, 
                 username=None, password=None, use_tls=False):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.attack_stats = {
            "anomalies_sent": 0,
            "anomalies_accepted": 0,
            "anomalies_rejected": 0,
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
    
    def generate_anomaly_payloads(self):
        """Generate various anomaly payloads"""
        anomalies = []
        
        # 1. Oversized payloads
        anomalies.append({
            "type": "oversized",
            "payload": "A" * 10000,  # 10KB payload
            "description": "Oversized payload (10KB)"
        })
        
        # 2. Malformed JSON
        anomalies.append({
            "type": "malformed_json",
            "payload": '{"invalid": json, "missing": quote}',
            "description": "Malformed JSON"
        })
        
        # 3. SQL Injection attempts
        anomalies.append({
            "type": "sql_injection",
            "payload": json.dumps({
                "value": "'; DROP TABLE users; --",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            "description": "SQL injection attempt"
        })
        
        # 4. XSS attempts
        anomalies.append({
            "type": "xss",
            "payload": json.dumps({
                "value": "<script>alert('XSS')</script>",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            "description": "XSS attempt"
        })
        
        # 5. Binary data
        anomalies.append({
            "type": "binary_data",
            "payload": bytes([0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD]),
            "description": "Binary data payload"
        })
        
        # 6. Unicode/Emoji attacks
        anomalies.append({
            "type": "unicode",
            "payload": json.dumps({
                "value": "",
                "unicode": "",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            "description": "Unicode/Emoji payload"
        })
        
        # 7. Null bytes
        anomalies.append({
            "type": "null_bytes",
            "payload": "normal\0\0\0hidden",
            "description": "Null byte injection"
        })
        
        # 8. Control characters
        anomalies.append({
            "type": "control_chars",
            "payload": "normal\x01\x02\x03\x04\x05control",
            "description": "Control character injection"
        })
        
        # 9. Nested objects (deep nesting)
        nested_obj = {"level": 0}
        current = nested_obj
        for i in range(1, 20):  # 20 levels deep
            current["nested"] = {"level": i}
            current = current["nested"]
        
        anomalies.append({
            "type": "deep_nesting",
            "payload": json.dumps(nested_obj),
            "description": "Deeply nested object"
        })
        
        # 10. Circular reference (will fail JSON serialization)
        anomalies.append({
            "type": "circular_ref",
            "payload": "circular_reference_test",
            "description": "Circular reference test"
        })
        
        # 11. Invalid data types
        anomalies.append({
            "type": "invalid_types",
            "payload": json.dumps({
                "value": float('inf'),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "invalid": float('nan')
            }),
            "description": "Invalid data types (inf, nan)"
        })
        
        # 12. Empty/null values
        anomalies.append({
            "type": "empty_values",
            "payload": json.dumps({
                "value": None,
                "empty": "",
                "null": None,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }),
            "description": "Empty/null values"
        })
        
        return anomalies
    
    def anomaly_worker(self, worker_id, anomalies, topics, delay_ms=1000):
        """Worker thread for payload anomaly attack"""
        client_id = f"anomaly_attacker_{worker_id}"
        client = self.create_client(client_id)
        
        if not client:
            self.attack_stats["connections_failed"] += 1
            return
        
        try:
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()
            
            print(f" Worker {worker_id}: Connected, starting payload anomaly attack...")
            
            # Send anomaly payloads
            for i, anomaly in enumerate(anomalies):
                try:
                    topic = random.choice(topics)
                    
                    print(f" Worker {worker_id}: Sending {anomaly['type']} - {anomaly['description']}")
                    
                    # Publish anomaly payload
                    result = client.publish(topic, anomaly['payload'], qos=0)
                    
                    self.attack_stats["anomalies_sent"] += 1
                    
                    if result.rc == 0:
                        self.attack_stats["anomalies_accepted"] += 1
                        print(f"  Worker {worker_id}: Anomaly accepted: {anomaly['type']}")
                    else:
                        self.attack_stats["anomalies_rejected"] += 1
                        print(f" Worker {worker_id}: Anomaly rejected: {anomaly['type']}")
                    
                    # Delay between anomalies
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)
                        
                except Exception as e:
                    self.attack_stats["anomalies_rejected"] += 1
                    print(f" Worker {worker_id}: Error sending {anomaly['type']}: {e}")
            
            print(f" Worker {worker_id}: Completed payload anomaly attack")
            
        except Exception as e:
            print(f" Worker {worker_id}: Connection failed: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
    
    def launch_attack(self, num_workers=2, delay_ms=1000, topics=None):
        """Launch payload anomaly attack"""
        if topics is None:
            topics = [
                "factory/tenantA/Temperature/telemetry",
                "factory/tenantA/Humidity/telemetry",
                "factory/tenantA/Motion/telemetry",
                "factory/tenantA/CO-Gas/telemetry",
                "factory/tenantA/Smoke/telemetry",
                "system/test/anomaly",
                "security/test/payload"
            ]
        
        anomalies = self.generate_anomaly_payloads()
        
        print(f" Starting Payload Anomaly Attack")
        print(f"   Workers: {num_workers}")
        print(f"   Anomaly types: {len(anomalies)}")
        print(f"   Delay: {delay_ms}ms")
        print(f"   Topics: {len(topics)}")
        print("=" * 60)
        
        self.attack_stats["start_time"] = time.time()
        
        # Create worker threads
        threads = []
        for i in range(num_workers):
            thread = threading.Thread(
                target=self.anomaly_worker,
                args=(i, anomalies, topics, delay_ms)
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
        print(f"Anomalies sent: {self.attack_stats['anomalies_sent']}")
        print(f"Anomalies accepted: {self.attack_stats['anomalies_accepted']}")
        print(f"Anomalies rejected: {self.attack_stats['anomalies_rejected']}")
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        
        if self.attack_stats['anomalies_sent'] > 0:
            acceptance_rate = (self.attack_stats['anomalies_accepted'] / 
                             self.attack_stats['anomalies_sent'] * 100)
            print(f"Anomaly acceptance rate: {acceptance_rate:.1f}%")
        
        if duration > 0:
            print(f"Anomalies per second: {self.attack_stats['anomalies_sent']/duration:.1f}")

def main():
    parser = argparse.ArgumentParser(description="MQTT Payload Anomaly Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--tls", action="store_true", help="Use TLS connection")
    parser.add_argument("--workers", type=int, default=2, help="Number of worker threads")
    parser.add_argument("--delay", type=int, default=1000, help="Delay between anomalies (ms)")
    parser.add_argument("--topics", nargs="+", help="Custom target topics")
    
    args = parser.parse_args()
    
    # Create attack instance
    attack = PayloadAnomalyAttack(
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
        topics=args.topics
    )

if __name__ == "__main__":
    main()
