"""
A simple example consuming messages from RabbitMQ.
"""

import asyncio
import logging

import amqpstorm_aio

logging.basicConfig(level=logging.INFO)

LOG = logging.getLogger(__name__)


async def on_message(message: amqpstorm_aio.Message) -> None:
    LOG.info("Message: %s", message.text)

    await message.ack()

    # Reject the message.
    # await message.reject(requeue=False)

    # Reject the message, and put it back in the queue.
    # await message.reject(requeue=True)


async def main() -> None:
    async with amqpstorm_aio.Connection("localhost", "guest", "guest") as connection:
        channel = await connection.channel()
        async with channel:
            await channel.queue.declare("example_queue", durable=True)
            await channel.basic.qos(prefetch_count=100)
            await channel.basic.consume(on_message, "example_queue", no_ack=False)
            await channel.start_consuming()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Consumer stopped.")
