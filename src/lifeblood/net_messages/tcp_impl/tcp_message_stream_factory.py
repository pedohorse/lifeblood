import asyncio
import struct
from ..interfaces import MessageStreamFactory
from ..stream_wrappers import MessageSendStream, MessageSendStreamBase
from ..address import DirectAddress, AddressChain


class TcpMessageStreamFactory(MessageStreamFactory):
    async def open_sending_stream(self, destination: DirectAddress, source: DirectAddress) -> MessageSendStreamBase:
        """
        address is expected to be in form of "1.2.3.4:1313|2.3.4.5:2424"...
        """
        host, sport = destination.split(':')
        reader, writer = await asyncio.open_connection(host, int(sport))
        source_bytes = source.encode('UTF-8')
        writer.write(struct.pack('>Q', len(source_bytes)))
        writer.write(source_bytes)
        await writer.drain()

        return MessageSendStream(reader, writer,
                                 reply_address=source,
                                 destination_address=destination)
