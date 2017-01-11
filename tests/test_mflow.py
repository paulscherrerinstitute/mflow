import unittest
import mflow
import mflow.handlers.array_1_0

import time
import json

from multiprocessing import Process, Queue

import logging
import numpy as np
import zmq

from mflow.handlers.array_1_0 import Handler


logger = logging.getLogger("mflow.mflow")
logger.setLevel(logging.DEBUG)


def sender(address, N, q):
    stream = mflow.connect(address, conn_type=mflow.BIND, mode=mflow.PUSH, queue_size=100, linger=100000)
    data = np.ones(10, dtype=np.int32)
    i = 0
    while i < N:
        try:
            header = {'htype': 'array-1.0', 'type': 'int32', 'shape': [10, ], 'frame': i}
            stream.send(json.dumps(header).encode('utf-8'), send_more=True, block=True)
            stream.send(data.tobytes(), block=True)
            print("SENT", i)
            i += 1

            # Send out every 10ms
            time.sleep(0.1)

        except KeyboardInterrupt:
            break

    stream.disconnect()
    return


def receiver(address, N, q):
    stream = mflow.connect(address, conn_type=mflow.CONNECT, mode=mflow.PULL, queue_size=100, linger=100000)  #receive_timeout=-1)
    #ctx = zmq.Context()
    #socket = ctx.socket(zmq.PULL)
    #socket.connect(address)
    i = 0
    while i < N:
        message = stream.receive(block=False)
        # message = stream.receive(block=False, handler=Handler().receive)
        #print(socket.recv_json())
        #print(socket.recv())
        print("message", message)
        if message.data is not None:
            if message.data["header"] is not None:
                print("DATA", message.data)
                i += 1
                q.put({"stat": message.statistics.messages_received, "counter": i})
            #time.sleep(0.1)
    stream.disconnect()
    return


class BaseTests(unittest.TestCase):
    def setUp(self):
        self.address = "tcp://127.0.0.1:4001"

    def tearDown(self):
        pass
    #time.sleep(1)

    def test_push_pull_recv_noblock(self):
        N = 10
        q = Queue()
        s = Process(target=sender, args=(self.address, N, q))
        s.start()

        time.sleep(1)
        r = Process(target=receiver, args=(self.address, N, q))
        r.start()


        s.join()
        time.sleep(1)
        r.terminate()
        i = 0
        stat = 0
        while not q.empty():
            data = q.get()
            i = data["counter"]
            stat = data["stat"]
        logger.info("%d %d" % (i, stat))
        self.assertEqual(N, i, 'Received too few messages')
        self.assertEqual(N, stat, 'Stats reports wrong number messages received')

