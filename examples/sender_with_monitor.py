import logging
import time

import numpy as np

import mflow
from mflow.tools import ConnectionCountMonitor


logger = logging.getLogger("mflow.mflow")
logger.setLevel(logging.ERROR)

address = "tcp://127.0.0.1:40000"

stream = mflow.connect(address, conn_type=mflow.BIND, mode=mflow.PUSH, receive_timeout=1, queue_size=1)

# Declare a callback function to call then the number of clients changes.
def print_number_of_connections(n_connections):
    print("Current number of clients: %d." % n_connections)

# Register the callback function to the ConnectionCountMonitor.
stream.register_socket_monitor(ConnectionCountMonitor(print_number_of_connections))

for i in range(16):
    try:
        header = '{"htype": "array-1.0", "type": "int32", "shape": [10], "frame": %d}' % i
        data = np.zeros(10, dtype=np.int32) + i

        stream.send(header.encode(), send_more=True, block=True)
        stream.send(data.tobytes(), block=False)

        print("Sending message %d" % i)
        # Send out every 10ms
        time.sleep(0.01)

    except KeyboardInterrupt:
        break

stream.disconnect()



