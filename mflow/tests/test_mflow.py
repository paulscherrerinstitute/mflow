import unittest
import mflow
import mflow.handlers.array_1_0

import time
import json

import numpy as np

from multiprocessing import Process, Queue

import logging


logger = logging.getLogger("mflow.mflow")
logger.setLevel(logging.DEBUG)


def sender(address, n, q, block=True):
    stream = mflow.connect(address, conn_type=mflow.BIND, mode=mflow.PUSH, queue_size=100, )
    data = np.ones(10, dtype=np.int32)
    data_size = len(data.tobytes())

    i = 0
    total_size = 0
    while i < n:
        try:
            header = {'htype': 'array-1.0', 'type': 'int32', 'shape': [10, ], 'frame': i}
            stream.send(json.dumps(header).encode('utf-8'), send_more=True, block=block)
            stream.send(data.tobytes(), block=block)
            i += 1
            total_size += data_size
            total_size += len(json.dumps(header).encode('utf-8'))
            q.put({'bytes_sent': data_size, 'total_sent': total_size})

            # Send out every 10ms
            time.sleep(0.2)

        except KeyboardInterrupt:
            break

    stream.disconnect()
    return


def sender_all(address, n, q, block=True):
    stream = mflow.connect(address, conn_type=mflow.BIND, mode=mflow.PUSH, queue_size=100, )
    data = np.ones(10, dtype=np.int32)
    data_size = len(data.tobytes())

    i = 0
    total_size = 0
    while i < n:
        try:
            header = {'htype': 'array-1.0', 'type': 'int32', 'shape': [10, ], 'frame': i}
            #stream.send(json.dumps(header).encode('utf-8'), send_more=True, block=block)
            stream.send_all(header, data.tobytes(), block=block)
            i += 1
            total_size += data_size
            total_size += len(json.dumps(header).encode('utf-8'))
            q.put({'bytes_sent': data_size, 'total_sent': total_size})

            # Send out every 10ms
            time.sleep(0.2)

        except KeyboardInterrupt:
            break

    stream.disconnect()
    return



def receiver(address, n, q, block=True):
    stream = mflow.connect(address, conn_type=mflow.CONNECT, mode=mflow.PULL, queue_size=100, )
    i = 0
    while i < n:
        message = stream.receive(block=block)
        if message:
            if message.data is not None:
                if message.data["header"] is not None:
                    i += 1
                    q.put({"total_sent": message.statistics.total_bytes_received,
                           "stat": message.statistics.messages_received, "counter": i, })
        #time.sleep(0.1)
    stream.disconnect()
    return


class BaseTests(unittest.TestCase):
    def setUp(self):
        self.address = "tcp://127.0.0.1:4001"

    def tearDown(self):
        pass

    def test_push_pull_recv_noblock(self):
        n = 10
        q = Queue()
        s = Process(target=sender, args=(self.address, n, q, ))
        s.start()

        time.sleep(0.1)
        q2 = Queue()
        r = Process(target=receiver, args=(self.address, n, q2, False))
        r.start()

        s.join()
        time.sleep(1)
        r.terminate()
        i = 0
        stat = 0
        while not q2.empty():
            data = q2.get()
            i = data["counter"]
            stat = data["stat"]
            total_recv = data["total_sent"]
        while not q.empty():
            data = q.get()
            total_size = data["total_sent"]

        self.assertEqual(n, i, 'Received too few messages')
        self.assertEqual(n, stat, 'Stats reports wrong number messages received')
        self.assertEqual(total_size, total_recv, 'Stats reports wrong number messages received about size')

    def test_push_pull_recv(self):
        n = 10
        q = Queue()
        s = Process(target=sender, args=(self.address, n, q))
        s.start()

        time.sleep(1)
        q2 = Queue()
        r = Process(target=receiver, args=(self.address, n, q2))
        r.start()

        s.join()
        time.sleep(2)
        r.terminate()
        i = 0
        stat = 0
        while not q2.empty():
            data = q2.get()
            i = data["counter"]
            stat = data["stat"]
            total_recv = data["total_sent"]
        while not q.empty():
            data = q.get()
            total_size = data["total_sent"]

        self.assertEqual(n, i, 'Received too few messages')
        self.assertEqual(n, stat, 'Stats reports wrong number messages received')
        self.assertEqual(total_size, total_recv, 'Stats reports wrong number messages received about size')

    def test_send_all(self):
        n = 10
        q = Queue()
        s = Process(target=sender_all, args=(self.address, n, q, ))
        s.start()

        time.sleep(0.1)
        q2 = Queue()
        r = Process(target=receiver, args=(self.address, n, q2, False))
        r.start()

        s.join()
        time.sleep(1)
        r.terminate()
        i = 0
        stat = 0
        while not q2.empty():
            data = q2.get()
            i = data["counter"]
            stat = data["stat"]
            total_recv = data["total_sent"]
        while not q.empty():
            data = q.get()
            total_size = data["total_sent"]

        self.assertEqual(n, i, 'Received too few messages')
        self.assertEqual(n, stat, 'Stats reports wrong number messages received')
        self.assertEqual(total_size, total_recv, 'Stats reports wrong number messages received about size')
