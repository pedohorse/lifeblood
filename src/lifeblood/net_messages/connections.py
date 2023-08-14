# import asyncio
# from .stream_wrappers import MessageStream
# from .address import split_address
#
# from typing import Tuple, Union
#
#
# async def open_sending_stream(address: Union[str, Tuple[Tuple[str, int], ...]], reply_address: Tuple[str, int]):
#     """
#     address is expected to be in form of "1.2.3.4:1313|2.3.4.5:2424"...
#     """
#     if isinstance(address, str):
#         address = split_address(address)
#     reader, writer = await asyncio.open_connection(address[0][0], address[0][1])
#
#     return MessageStream(reader, writer, reply_address)
