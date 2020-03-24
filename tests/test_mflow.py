import threading
import unittest
from itertools import groupby

import mflow
import mflow.handlers.array_1_0

import time
import json

import numpy as np

from multiprocessing import Process, Queue

import logging

from mflow.tools import ConnectionCountMonitor

logger = logging.getLogger("mflow.mflow")
logger.setLevel(logging.DEBUG)


def sender(address, n, q, block=True, copy=True):
    stream = mflow.connect(address, conn_type=mflow.BIND, mode=mflow.PUSH, queue_size=100, copy=copy)
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


def receiver(address, n, q, block=True, copy=True):
    stream = mflow.connect(address, conn_type=mflow.CONNECT, mode=mflow.PULL, queue_size=100, copy=copy)
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

    def test_push_pull_recv_nocopy(self):
        n = 10
        q = Queue()
        s = Process(target=sender, args=(self.address, n, q, True, False))
        s.start()

        time.sleep(0.1)
        q2 = Queue()
        r = Process(target=receiver, args=(self.address, n, q2, True, False))
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

    def test_socket_monitor_sender(self):
        socket_address = "tcp://127.0.0.1:9999"
        n_connected_clients = []

        def process_client_count_change(n_clients):
            n_connected_clients.append(n_clients)

        try:
            sending_stream = mflow.Stream()
            sending_stream.register_socket_monitor(ConnectionCountMonitor(process_client_count_change))
            sending_stream.connect(address=socket_address, conn_type=mflow.BIND, mode=mflow.PUSH)
            # Lets give it some time to settle.
            time.sleep(1)

            self.assertEqual(n_connected_clients[-1], 0, "Initial value of 0 not sent to callback.")

            receiving_stream_1 = mflow.Stream()
            receiving_stream_1.connect(address=socket_address, conn_type=mflow.CONNECT, mode=mflow.PULL)
            # Lets give it some time to process the event.
            time.sleep(0.1)
            self.assertEqual(n_connected_clients[-1], 1, "New client connection not reported.")

            receiving_stream_2 = mflow.Stream()
            receiving_stream_2.connect(address=socket_address, conn_type=mflow.CONNECT, mode=mflow.PULL)
            # Lets give it some time to process the event.
            time.sleep(0.1)
            self.assertEqual(n_connected_clients[-1], 2, "New client connection not reported.")

            receiving_stream_2.disconnect()
            # Lets give it some time to process the event.
            time.sleep(0.1)
            self.assertEqual(n_connected_clients[-1], 1, "Client disconnect not reported.")

            receiving_stream_1.disconnect()
            # Lets give it some time to process the event.
            time.sleep(0.1)
            self.assertEqual(n_connected_clients[-1], 0, "Client disconnect not reported.")

            # We need to filter the duplicate states - used as heartbeat for time counting.
            processed_n_connected_clients = [x[0] for x in groupby(n_connected_clients)]

            # Check overall client number report.
            self.assertEqual(processed_n_connected_clients, [0, 1, 2, 1, 0], "Wrong number of clients reported.")

        finally:
            sending_stream.disconnect()

    def test_socket_monitor_dynamic(self):
        socket_address = "tcp://127.0.0.1:9999"
        n_connected_clients = []

        # Increase the polling interval, to make tests faster.
        mflow.SocketEventListener.DEFAULT_SOCKET_RECEIVE_TIMEOUT = 0.1

        def process_client_count_change(n_clients):
            # Append only value that changed.
            if not n_connected_clients or n_connected_clients[-1] != n_clients:
                n_connected_clients.append(n_clients)

        try:
            sending_stream = mflow.Stream()
            sending_stream.connect(address=socket_address, conn_type=mflow.BIND, mode=mflow.PUSH)

            receiving_stream_1 = mflow.Stream()
            receiving_stream_1.connect(address=socket_address, conn_type=mflow.CONNECT, mode=mflow.PULL)
            # Lets give it some time to forget about the receiver 1 connection event.
            time.sleep(0.2)

            socket_monitor = ConnectionCountMonitor(process_client_count_change)
            sending_stream.register_socket_monitor(socket_monitor)
            time.sleep(0.1)

            receiving_stream_2 = mflow.Stream()
            receiving_stream_2.connect(address=socket_address, conn_type=mflow.CONNECT, mode=mflow.PULL)
            # Lets give it some time to process the event.
            time.sleep(0.1)

            receiving_stream_1.disconnect()
            receiving_stream_2.disconnect()
            time.sleep(0.1)

            sending_stream.remove_socket_monitor(socket_monitor)
            # Wait for the RECV timeout to run out.
            time.sleep(0.1)

            # This should not be reported, because we removed the listener.
            receiving_stream_3 = mflow.Stream()
            receiving_stream_3.connect(address=socket_address, conn_type=mflow.CONNECT, mode=mflow.PULL)
            time.sleep(0.1)

            # Check overall client number report.
            self.assertEqual(n_connected_clients, [0, 1, 0, -1], "Wrong number of clients reported.")
        finally:
            # sending_stream.disconnect()
            pass

    def test_no_client_monitor(self):
        socket_address = "tcp://127.0.0.1:9999"
        no_clients_triggered = False
        # Increase the polling interval, to make tests faster.
        mflow.SocketEventListener.DEFAULT_SOCKET_RECEIVE_TIMEOUT = 0.01

        def no_clients():
            nonlocal no_clients_triggered
            no_clients_triggered = True

        server = mflow.connect(address=socket_address, conn_type=mflow.BIND, mode=mflow.PUSH,
                               no_client_action=no_clients, no_client_timeout=0.3)

        self.assertFalse(no_clients_triggered, "Timeout triggered too fast.")
        time.sleep(0.2)
        self.assertFalse(no_clients_triggered, "Timeout triggered too fast.")
        time.sleep(0.3)
        self.assertTrue(no_clients_triggered, "Did not trigger timeout if no clients connected.")

        # Reset the test.
        no_clients_triggered = False

        # First client.
        client_1 = mflow.connect(socket_address)
        time.sleep(0.4)
        self.assertFalse(no_clients_triggered, "Callback triggered even with connected clients.")
        client_1.disconnect()

        # Wait a bit and verify the triggering status.
        time.sleep(0.1)
        self.assertFalse(no_clients_triggered, "Timeout triggered too fast.")
        time.sleep(0.3)
        self.assertTrue(no_clients_triggered, "Callback did not re-trigger.")

        server.disconnect()

    def test_send_timeout(self):

        send_timeout = 100

        def test_timeout():
            socket_address = "tcp://127.0.0.1:9999"
            server = mflow.connect(address=socket_address, conn_type=mflow.BIND, mode=mflow.PUSH,
                                   send_timeout=send_timeout)

            server.send(message={"valid": True}, block=True, as_json=True)

        # Run it in a separate process, so we can terminate it if needed.
        process = Process(target=test_timeout)
        process.start()

        # Lets wait a bit more.
        time.sleep(send_timeout/1000 + 0.5)

        is_process_alive = process.is_alive()
        process.terminate()

        self.assertFalse(is_process_alive, "Process should die automatically.")
