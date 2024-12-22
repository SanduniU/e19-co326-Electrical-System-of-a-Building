from flask import Flask, request, jsonify
import pika
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# RabbitMQ (AMQP) Configuration
AMQP_BROKER = "amqp://guest:guest@localhost:5672/vhost"
AMQP_QUEUE = "amqp_data"

# RabbitMQ connection and channel
amqp_connection = None
amqp_channel = None

# Flask application
app = Flask(__name__)

# Function to connect to RabbitMQ
def connect_to_amqp():
    global amqp_connection, amqp_channel
    try:
        amqp_connection = pika.BlockingConnection(pika.ConnectionParameters(host=AMQP_BROKER))
        amqp_channel = amqp_connection.channel()
        amqp_channel.queue_declare(queue=AMQP_QUEUE)
        logging.info("Connected to RabbitMQ")
    except Exception as e:
        logging.error(f"Error connecting to RabbitMQ: {e}")
        connect_to_amqp()

# Publish a message to RabbitMQ (AMQP)
def publish_to_amqp(message):
    try:
        if not amqp_connection or amqp_connection.is_closed:
            connect_to_amqp()
        amqp_channel.basic_publish(exchange='', routing_key=AMQP_QUEUE, body=message)
        logging.info(f"Published to AMQP: {message}")
    except Exception as e:
        logging.error(f"Error publishing to AMQP: {e}")
        connect_to_amqp()

# HTTP POST Endpoint
@app.route("/data", methods=["POST"])
def handle_http_post():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
        
        payload = json.dumps(data)
        logging.info(f"Received HTTP POST message: {payload}")
        publish_to_amqp(payload)
        return jsonify({"message": "Message forwarded to AMQP"}), 200
    except Exception as e:
        logging.error(f"Error processing HTTP message: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

# Start the Flask server
if __name__ == "__main__":
    connect_to_amqp()
    try:
        app.run(host="0.0.0.0", port=5000)
    except KeyboardInterrupt:
        logging.info("Shutting down HTTP-to-AMQP converter...")
        if amqp_connection and not amqp_connection.is_closed:
            amqp_connection.close()
