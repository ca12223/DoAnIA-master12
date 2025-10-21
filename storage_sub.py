#!/usr/bin/env python3
# file: office_sub.py
import argparse, json, uuid
import paho.mqtt.client as mqtt

def build_client_id(prefix="truongphong_storage"):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"

def on_connect(client, userdata, flags, reason_code, properties=None):
    print("Connected:", reason_code)
    rc, mid = client.subscribe("factory/storage/+/telemetry", qos=1)
    print("SUBSCRIBE sent:", rc, "mid=", mid)

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("SUBACK granted_qos =", granted_qos)
    if any(q == 128 for q in granted_qos):
        print("=> BỊ TỪ CHỐI (kiểm tra ACL cho user này)")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        body = json.dumps(payload, ensure_ascii=False)
    except Exception:
        body = msg.payload[:200].decode("utf-8", errors="ignore")
    print(f"[{msg.topic}] QoS={msg.qos} retain={int(msg.retain)} -> {body}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default="192.168.101.144")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--username", default="truongphong_storage")
    ap.add_argument("--password", default="123")
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--cafile", default=None)
    ap.add_argument("--insecure", action="store_true")
    args = ap.parse_args()

   
    try:
        client = mqtt.Client(
            client_id=build_client_id(),
            protocol=mqtt.MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.v5  
        )
    except Exception:
        
        client = mqtt.Client(client_id=build_client_id(), protocol=mqtt.MQTTv5)

    client.username_pw_set(args.username, args.password)

    if args.tls:
        import ssl
        if args.cafile:
            client.tls_set(ca_certs=args.cafile, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        else:
            client.tls_set()
        if args.insecure:
            client.tls_insecure_set(True)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message

    client.connect(args.broker, args.port, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()