import asyncio
from ..interfaces import MessageStreamFactory
from ..stream_wrappers import MessageStream
from ..address import DirectAddress, AddressChain


class TcpMessageStreamFactory(MessageStreamFactory):
    async def open_message_connection(self, destination: DirectAddress, source: AddressChain) -> MessageStream:
        """
        address is expected to be in form of "1.2.3.4:1313|2.3.4.5:2424"...
        """
        host, sport = destination.split(':')
        reader, writer = await asyncio.open_connection(host, int(sport))

        return MessageStream(reader, writer, source)
