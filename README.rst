amqpstorm-aio
=============

Asyncio-only, high-performance AMQP 0-9-1 client for Python 3.14+.

The spiritual successor to `AMQPStorm <https://github.com/eandersson/amqpstorm>`_,
rebuilt from the ground up for asyncio.

Usage
-----

.. code-block:: python

    import asyncio

    import amqpstorm_aio


    async def main() -> None:
        async with amqpstorm_aio.Connection("localhost", "guest", "guest") as connection:
            channel = await connection.channel()
            async with channel:
                await channel.queue.declare("simple_queue")
                await channel.confirm_deliveries()
                await channel.basic.publish(
                    "Hello World!",
                    routing_key="simple_queue",
                    properties={"content_type": "text/plain", "delivery_mode": 2},
                )

                await channel.basic.consume(queue="simple_queue")
                async for message in channel.build_inbound_messages(break_on_empty=True):
                    print(message.text)
                    await message.ack()


    asyncio.run(main())

Clusters
--------

Pass a list of hostnames or a round robin DNS record to connect to whichever node answers. Each connection
shuffles the list, so a fleet of clients spreads across the cluster instead of every
one of them starting at the same node. Within a connection, hosts are then tried in
order -- each with its own ``timeout`` -- and the starting position rotates on every
``open()`` so reconnects keep spreading:

.. code-block:: python

    connection = amqpstorm_aio.Connection(
        ["rmq-a.example.net", "rmq-b.example.net", "rmq-c.example.net"],
        "guest",
        "guest",
    )

The same thing in URI form, with one port shared by every host:

.. code-block:: python

    connection = amqpstorm_aio.UriConnection(
        "amqp://guest:guest@rmq-a,rmq-b,rmq-c:5672/%2F"
    )

If no node answers, the error names every host that was tried. Over TLS each node's
certificate is matched against that node's own hostname unless
``ssl_options["server_hostname"]`` overrides it.

A single hostname that resolves to several addresses -- round-robin DNS, or a
Kubernetes headless service -- spreads the same way. Every hostname is resolved and
its addresses shuffled per connection, so clients balance across the nodes behind one
name rather than all taking whichever address the resolver lists first, which caching
tends to keep constant for the life of a process.

Regenerating the protocol layer
-------------------------------

``amqpstorm_aio/spec.py`` is generated -- do not edit it by hand::

    python codegen/generate.py

