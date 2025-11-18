import sys
import os
import time

try:
    import mflow
except:
    sys.path.append(os.environ["PWD"] + "/../")
    import mflow

import logging
import numpy as np

logger = logging.getLogger("mflow.mflow")
logger.setLevel(logging.ERROR)

address = "tcp://127.0.0.1:40000"

stream = mflow.connect(address, conn_type=mflow.BIND, mode=mflow.PUSH, receive_timeout=1, queue_size=1)


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



