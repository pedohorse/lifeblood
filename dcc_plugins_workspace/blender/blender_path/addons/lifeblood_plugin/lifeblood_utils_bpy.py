import threading
import time
from queue import Queue
from . import lifeblood_utils
from typing import Tuple
import bpy


def address_from_broadcast_ui(broadcast_address: Tuple[str, int], awaited_id='lifeblood_scheduler', timeout=10):
    ret_queue = Queue()
    thread = threading.Thread(target=lambda: ret_queue.put(
        lifeblood_utils.address_from_broadcast(
            broadcast_address,
            awaited_id=awaited_id,
            timeout=timeout)))
    thread.start()

    time_step = 0.1
    start_time = time.time()
    wm = bpy.context.window_manager
    wm.progress_begin(0, timeout)
    for _ in range(int(timeout/time_step + 1) + 2):  # +2 for good measure
        wm.progress_update(time.time() - start_time)
        thread.join(timeout=time_step)
        if thread.is_alive():
            continue
        if ret_queue.qsize() == 0:  # means error happened in the thread
            raise RuntimeError('error listening to the broadcast!')
        address, port = ret_queue.get()
        wm.progress_end()
        return address, port

    wm.progress_end()
    return None, None
