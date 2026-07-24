import asyncio
from collections.abc import Callable
from typing import Any
from typing import Self
from typing import cast

from amqpstorm_aio import framing
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory

DEFAULT_SERVER_PROPERTIES: dict[str, Any] = {
    "product": "FakeBroker",
    "capabilities": {"basic.nack": True, "publisher_confirms": True},
}

type Responder = Callable[["BrokerSession", int, spec.Method], None]
type _PendingContent = tuple[int, spec.Basic.Publish, framing.ContentHeader | None, bytearray]


class BrokerSession:
    """One accepted client connection inside the fake broker."""

    def __init__(self, broker: FakeBroker, writer: asyncio.StreamWriter) -> None:
        self.broker = broker
        self.writer = writer
        self.confirming: set[int] = set()
        self.delivery_tags: dict[int, int] = {}
        self.received: list[tuple[int, object]] = []
        self.published: list[tuple[int, spec.Basic.Publish, bytes]] = []
        self._content: _PendingContent | None = None

    def send_method(self, channel_id: int, method: spec.Method) -> None:
        buffer = bytearray()
        framing.append_method_frame(buffer, channel_id, method)
        self.writer.write(bytes(buffer))

    def send_content(
        self,
        channel_id: int,
        method: spec.Method,
        body: bytes,
        properties: spec.Basic.Properties | None = None,
    ) -> None:
        buffer = bytearray()
        framing.append_method_frame(buffer, channel_id, method)
        framing.append_content_header_frame(
            buffer, channel_id, len(body), properties or spec.Basic.Properties()
        )
        if body:
            framing.append_frame(buffer, spec.FRAME_BODY, channel_id, body)
        self.writer.write(bytes(buffer))

    def close_transport(self) -> None:
        self.writer.close()

    def on_frame(self, channel_id: int, frame: framing.FrameValue) -> None:
        self.received.append((channel_id, frame))
        if isinstance(frame, framing.ContentHeader):
            if self._content is not None:
                self._content = (self._content[0], self._content[1], frame, self._content[3])
                if frame.body_size == 0:
                    self._finish_content()
            return
        if isinstance(frame, bytes):
            if self._content is not None and self._content[2] is not None:
                self._content[3].extend(frame)
                if len(self._content[3]) >= self._content[2].body_size:
                    self._finish_content()
            return
        if isinstance(frame, framing.Heartbeat):
            self.writer.write(framing.HEARTBEAT_FRAME)
            return
        method = cast(spec.Method, frame)
        responder = self.broker.responders.get(method.index)
        if responder is not None:
            responder(self, channel_id, method)
            return
        self._default_respond(channel_id, method)

    def _default_respond(self, channel_id: int, frame: spec.Method) -> None:
        index = frame.index
        if index == spec.Connection.StartOk.index:
            self.send_method(
                0,
                factory.build(spec.Connection.Tune,
                    channel_max=self.broker.channel_max,
                    frame_max=self.broker.frame_max,
                    heartbeat=self.broker.heartbeat,
                ),
            )
        elif index == spec.Connection.TuneOk.index:
            pass
        elif index == spec.Connection.Open.index:
            self.send_method(0, factory.build(spec.Connection.OpenOk))
        elif index == spec.Connection.Close.index:
            self.send_method(0, factory.build(spec.Connection.CloseOk))
        elif index == spec.Connection.CloseOk.index:
            pass
        elif index == spec.Channel.Open.index:
            self.send_method(channel_id, factory.build(spec.Channel.OpenOk))
        elif index == spec.Channel.Close.index:
            self.send_method(channel_id, factory.build(spec.Channel.CloseOk))
        elif index == spec.Channel.CloseOk.index:
            pass
        elif index == spec.Exchange.Declare.index:
            self.send_method(channel_id, factory.build(spec.Exchange.DeclareOk))
        elif index == spec.Exchange.Delete.index:
            self.send_method(channel_id, factory.build(spec.Exchange.DeleteOk))
        elif index == spec.Exchange.Bind.index:
            self.send_method(channel_id, factory.build(spec.Exchange.BindOk))
        elif index == spec.Exchange.Unbind.index:
            self.send_method(channel_id, factory.build(spec.Exchange.UnbindOk))
        elif index == spec.Queue.Declare.index:
            declare = cast(spec.Queue.Declare, frame)
            self.send_method(
                channel_id,
                factory.build(spec.Queue.DeclareOk,
                    queue=declare.queue or "generated.q", message_count=0, consumer_count=0
                ),
            )
        elif index == spec.Queue.Bind.index:
            self.send_method(channel_id, factory.build(spec.Queue.BindOk))
        elif index == spec.Queue.Unbind.index:
            self.send_method(channel_id, factory.build(spec.Queue.UnbindOk))
        elif index == spec.Queue.Purge.index:
            self.send_method(channel_id, factory.build(spec.Queue.PurgeOk, message_count=0))
        elif index == spec.Queue.Delete.index:
            self.send_method(channel_id, factory.build(spec.Queue.DeleteOk, message_count=0))
        elif index == spec.Confirm.Select.index:
            self.confirming.add(channel_id)
            self.delivery_tags[channel_id] = 0
            self.send_method(channel_id, factory.build(spec.Confirm.SelectOk))
        elif index == spec.Tx.Select.index:
            self.send_method(channel_id, factory.build(spec.Tx.SelectOk))
        elif index == spec.Tx.Commit.index:
            self.send_method(channel_id, factory.build(spec.Tx.CommitOk))
        elif index == spec.Tx.Rollback.index:
            self.send_method(channel_id, factory.build(spec.Tx.RollbackOk))
        elif index == spec.Basic.Qos.index:
            self.send_method(channel_id, factory.build(spec.Basic.QosOk))
        elif index == spec.Basic.Recover.index:
            self.send_method(channel_id, factory.build(spec.Basic.RecoverOk))
        elif index == spec.Basic.Publish.index:
            publish = cast(spec.Basic.Publish, frame)
            self._content = (channel_id, publish, None, bytearray())
        elif index == spec.Basic.Consume.index:
            consume = cast(spec.Basic.Consume, frame)
            tag = consume.consumer_tag or f"fake-tag-{len(self.received)}"
            self.send_method(channel_id, factory.build(spec.Basic.ConsumeOk, consumer_tag=tag))
        elif index == spec.Basic.Cancel.index:
            cancel = cast(spec.Basic.Cancel, frame)
            self.send_method(
                channel_id,
                factory.build(spec.Basic.CancelOk, consumer_tag=cancel.consumer_tag),
            )
        elif index == spec.Basic.Get.index:
            self.send_method(channel_id, factory.build(spec.Basic.GetEmpty))
        elif index in (
            spec.Basic.Ack.index,
            spec.Basic.Nack.index,
            spec.Basic.Reject.index,
            spec.Channel.FlowOk.index,
        ):
            pass
        else:
            raise RuntimeError(f"fake broker has no default response for {frame.name}")

    def _finish_content(self) -> None:
        if self._content is None:
            return
        channel_id, publish, _header, body = self._content
        self._content = None
        self.published.append((channel_id, publish, bytes(body)))
        hook = self.broker.on_publish
        if hook is not None:
            hook(self, channel_id, publish, bytes(body))
        if channel_id in self.confirming:
            self.delivery_tags[channel_id] += 1
            self.send_method(
                channel_id,
                factory.build(
                    spec.Basic.Ack, delivery_tag=self.delivery_tags[channel_id]
                ),
            )


class FakeBroker:
    """Scripted broker; ``async with FakeBroker() as broker`` yields a listening port."""

    def __init__(
        self,
        channel_max: int = 2047,
        frame_max: int = 131_072,
        heartbeat: int = 60,
        mechanisms: bytes = b"PLAIN AMQPLAIN",
    ) -> None:
        self.channel_max = channel_max
        self.frame_max = frame_max
        self.heartbeat = heartbeat
        self.mechanisms = mechanisms
        self.responders: dict[int, Responder] = {}
        self.on_publish: Callable[[BrokerSession, int, spec.Basic.Publish, bytes], None] | None = (
            None
        )
        self.sessions: list[BrokerSession] = []
        self.port = 0
        self._server: asyncio.Server | None = None

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.stop()

    async def start(self, host: str = "127.0.0.1") -> None:
        """Listen on an ephemeral port; ``host`` picks which address to bind."""
        self._server = await asyncio.start_server(self._handle_client, host, 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        session = BrokerSession(self, writer)
        self.sessions.append(session)
        try:
            preamble = await reader.readexactly(8)
            if preamble != framing.PROTOCOL_HEADER:
                writer.write(framing.PROTOCOL_HEADER)
                return
            session.send_method(
                0,
                factory.build(spec.Connection.Start,
                    version_major=0,
                    version_minor=9,
                    server_properties=dict(DEFAULT_SERVER_PROPERTIES),
                    mechanisms=self.mechanisms,
                    locales=b"en_US",
                ),
            )
            parser = framing.FrameParser(session.on_frame, 1_048_576)
            while True:
                data = await reader.read(65_536)
                if not data:
                    return
                parser.feed(data)
                await writer.drain()
        except asyncio.IncompleteReadError, ConnectionResetError, ConnectionAbortedError, OSError:
            return
        finally:
            writer.close()
