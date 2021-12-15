import asyncio
from unittest import IsolatedAsyncioTestCase
from lifeblood import broadcasting
from lifeblood import nethelpers


class InterfaceTests(IsolatedAsyncioTestCase):

    async def test_broadcast(self):
        async def _broad_receiver():
            nonlocal msg_received
            for _ in range(3):
                self.assertEqual('ooh, fresh information!', await broadcasting.await_broadcast('test me', 9271))
                msg_received += 1
            print('all received')

        msg_received = 0
        listener = asyncio.create_task(_broad_receiver())

        # i need to be super sure that received is started. though there has never been observed race conditions in this test here, i still just feel better putting here some extra sleep.
        await asyncio.sleep(1)
        # TODO: even though it's highly unlikely to be a problem - still better think of a more reliable way of waiting for listener to listen

        _, caster = await broadcasting.create_broadcaster('test me', 'ooh, fresh information!', ip=nethelpers.get_default_broadcast_addr(), broad_port=9271, broadcasts_count=3, broadcast_interval=3)
        await caster.till_done()
        await listener
        self.assertEqual(3, msg_received)

