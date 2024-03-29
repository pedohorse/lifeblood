import random
import socket
import threading
from math import ceil
from unittest import TestCase
from lifeblood import buffered_connection, nethelpers

from typing import Optional


class BufferedConnectionTests(TestCase):
    def setUp(self) -> None:
        rng = random.Random(666)
        self.random_bytes = b''.join(rng.randint(0, 255).to_bytes(1, byteorder='big') for _ in range(5678))

    def _get_random_chunk(self, chunk_size, rng=None):
        """
        helper that gets random shit from pregenerated rando_bytes
        """
        if rng is None:
            rng = random.Random(1313666)
        samplesize = len(self.random_bytes)
        offset = rng.randint(0, samplesize-1)
        mult = int(ceil((offset + chunk_size)/samplesize))
        assert mult > 0
        return (self.random_bytes if mult == 1 else self.random_bytes*mult)[offset:offset + chunk_size]

    def test_buffered_connection_tiny(self):
        self._helper_buffered_connection_test(8, 10, 8, 10, port=1234)

    def test_buffered_connection_small(self):
        self._helper_buffered_connection_test(32, 1024, 64, None, port=1235)

    def test_buffered_connection_med(self):
        self._helper_buffered_connection_test(64, 8192, 256, None, port=1236)

    def test_buffered_connection_large(self):
        self._helper_buffered_connection_test(256, 2**15, 2048, None, port=1237)

    def test_buffered_connection_huge(self):
        self._helper_buffered_connection_test(2**15, 2**10, 2**20, None, port=1237)

    def _helper_buffered_connection_test(self, chunk_size=8, parts=10, alternate_chunk_size=10, alternate_parts: Optional[int]=8, *, port=1234):
        if alternate_parts is None:
            alternate_parts = chunk_size * parts // alternate_chunk_size
        assert alternate_chunk_size * alternate_parts == chunk_size * parts

        def _thread_body():
            rwsock, raddr = sock.accept()
            print(rwsock, raddr)
            def _subreader():
                total_read = 0
                while total_read < parts*chunk_size:
                    # time.sleep(random.uniform(0, 0.1))  # do i need to imitate lag at all?
                    if total_read < parts * chunk_size:
                        print('bob reading')
                        data_in = rwsock.recv(4096)
                        print(f'bob read {len(data_in)}')
                        total_read += len(data_in)
                        read_parts.append(data_in)
                print(f'total read {total_read}')

            def _subwriter():
                total_written = 0
                curr_write_part = 0
                while curr_write_part < len(write_parts):
                    #time.sleep(random.uniform(0, 0.1))  # do i need to imitate lag at all?
                    if curr_write_part < len(write_parts):
                        print(f'bob writing {repr(write_parts[curr_write_part])[:16]}...')
                        rwsock.sendall(write_parts[curr_write_part])
                        total_written += len(write_parts[curr_write_part])
                        print(f'bob wrote {len(write_parts[curr_write_part])}')
                        curr_write_part += 1
                    print(f'total write {total_written}')

            subr = threading.Thread(target=_subreader)
            subw = threading.Thread(target=_subwriter)
            subr.start()
            subw.start()
            subr.join()
            subw.join()
            rwsock.close()

        # init data
        print('generating test data...')
        rng = random.Random(1313666)
        read_crap = []
        write_crap = [self._get_random_chunk(chunk_size, rng=rng) for _ in range(parts)]
        read_parts = []
        write_parts = [self._get_random_chunk(alternate_chunk_size, rng=rng) for _ in range(alternate_parts)]
        print('test data generated')
        #

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr = None
        for attempt in range(64):
            addr = (nethelpers.get_localhost(), 1234 + attempt)
            try:
                sock.bind(addr)
                break
            except:
                continue
        else:
            raise RuntimeError('could not find an open port to test')
        sock.listen(0)

        thread = threading.Thread(target=_thread_body)
        thread.start()

        con = buffered_connection.BufferedConnection(addr)

        for write_part in write_crap:
            print(f'alice reading {chunk_size}')
            read_crap.append(con.reader.read(chunk_size))
            print(f'alice read {repr(read_crap[-1])[:16]}...')
            print(f'alice writing {repr(write_part)[:16]}...')
            con.writer.write(write_part)
            con.writer.flush()

        thread.join(timeout=10)
        good = not thread.is_alive()
        sock.close()
        con.close()
        self.assertTrue(good)

        self.assertEqual(b''.join(write_crap), b''.join(read_parts))
        self.assertEqual(b''.join(read_crap), b''.join(write_parts))
