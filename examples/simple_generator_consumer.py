"""
A simple example consuming messages from RabbitMQ with an async for loop.
"""

import asyncio
import logging

import amqpstorm_aio

logging.basicConfig(level=logging.INFO)

LOG = logging.getLogger(__name__)

async def main() -> None:
    async with amqpstorm_aio.Connection("localhost", "guest", "guest") as connection:
        channel = await connection.channel()
        async with channel:
            await channel.queue.declare("example_queue", durable=True)
            await channel.basic.qos(prefetch_count=100)
            await channel.basic.consume(queue="example_queue", no_ack=False)

            async for message in channel:
                LOG.info(message.text)
                await message.ack()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info("Consumer stopped.")
