import asyncio
from aiocoap import * 
import pika
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# RabbitMQ (AMQP) Configuration
AMQP_BROKER = "amqp.example.com"
AMQP_QUEUE = "amqp_data"

# RabbitMQ connection and channel
amqp_connection = None
amqp_channel = None

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
        asyncio.sleep(5)  # Retry delay
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

# CoAP Server Resource
class CoAPResource(resource.Resource):
    async def render_post(self, request):
        try:
            payload = request.payload.decode("utf-8")
            logging.info(f"Received CoAP POST message: {payload}")
            publish_to_amqp(payload)
            return Message(code=CHANGED, payload=b"Message forwarded to AMQP")
        except Exception as e:
            logging.error(f"Error processing CoAP message: {e}")
            return Message(code=INTERNAL_SERVER_ERROR, payload=b"Error forwarding to AMQP")

# Start the CoAP Server
async def main():
    connect_to_amqp()
    root = resource.Site()
    root.add_resource(["sensor", "data"], CoAPResource())
    await Context.create_server_context(root)
    logging.info("CoAP server running...")
    await asyncio.get_running_loop().create_future()  # Keep running

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down CoAP-to-AMQP converter...")
        if amqp_connection and not amqp_connection.is_closed:
            amqp_connection.close()
