import paho.mqtt.client as mqtt

def on_message(client, userdata, msg):
    print(f"{msg.topic}: {msg.payload.decode()}")

# ✅ Updated for paho-mqtt 2.x
client = mqtt.Client(client_id="subscriber", callback_api_version=1)

client.on_message = on_message
client.connect("localhost", 1883)

# subscribe to your topic (use correct topic name)
client.subscribe("factory/tenantA//telemetry")

print("Subscriber connected and waiting for messages...")
client.loop_forever()