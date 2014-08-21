"""
A simple example publishing a message to RabbitMQ.
"""

import asyncio
import logging

import amqpstorm_aio

logging.basicConfig(level=logging.DEBUG)

LOG = logging.getLogger(__name__)


async def main() -> None:
    async with amqpstorm_aio.Connection(
        "localhost", "guest", "guest"
    ) as connection:
        channel = await connection.channel()
        async with channel:
            await channel.queue.declare("example_queue")
            await channel.confirm_deliveries()

            properties = amqpstorm_aio.Properties(
                content_type="text/plain",
                delivery_mode=2,
                headers={"key": "value"},
            )

            confirmed = await channel.basic.publish(
                "Hello World!",
                routing_key="example_queue",
                properties=properties,
            )
            LOG.info(f"Confirmed: {confirmed}")


if __name__ == "__main__":
    asyncio.run(main())
