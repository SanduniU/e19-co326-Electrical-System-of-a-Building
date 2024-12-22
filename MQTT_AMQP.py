import paho.mqtt.client as mqtt
import pika
import json

# MQTT Configuration
MQTT_BROKER = "mqtt.example.com"
MQTT_PORT = 1883
MQTT_TOPIC = "sensor/data"

# RabbitMQ Configuration
AMQP_BROKER = "amqp.example.com"
AMQP_QUEUE = "amqp_data"

# Callback when an MQTT message is received
def on_mqtt_message(client, userdata, msg):
    try:
        # Decode and parse the MQTT message
        payload = msg.payload.decode("utf-8")
        print(f"Received MQTT message: {payload}")
        
        # Publish the message to AMQP
        publish_to_amqp(payload)
    except Exception as e:
        print(f"Error processing MQTT message: {e}")

# Publish a message to RabbitMQ (AMQP)
def publish_to_amqp(message):
    try:
        # Establish connection to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=AMQP_BROKER))
        channel = connection.channel()

        # Declare the queue
        channel.queue_declare(queue=AMQP_QUEUE)

        # Publish the message
        channel.basic_publish(exchange='', routing_key=AMQP_QUEUE, body=message)
        print(f"Published to AMQP: {message}")

        # Close the connection
        connection.close()
    except Exception as e:
        print(f"Error publishing to AMQP: {e}")

# MQTT Setup
mqtt_client = mqtt.Client()
mqtt_client.on_message = on_mqtt_message

# Connect to MQTT Broker
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.subscribe(MQTT_TOPIC)
print(f"Subscribed to MQTT topic: {MQTT_TOPIC}")

# Start the MQTT client loop
try:
    mqtt_client.loop_forever()
except KeyboardInterrupt:
    print("Shutting down converter...")
    mqtt_client.disconnect()

