import sys
import os

try:
    import mflow
except:
    sys.path.append(os.environ["PWD"] + "/../")
    import mflow

import logging
import numpy as np


logger = logging.getLogger("mflow.mflow")
logger.setLevel(logging.DEBUG)

address = "tcp://127.0.0.1:40000"

stream = mflow.connect(address, conn_type=mflow.BIND, mode=mflow.PUSH, receive_timeout=1, queue_size=1000)

data = np.ones(10, dtype=np.int32)
i = 0
while True:
    try:
        header = '{"htype": "array-1.0", "type": "int32", "shape": [10], "frame": %d}' % i
        stream.send(header.encode(), send_more=True)
        stream.send(data.tobytes(), send_more=False)
        i += 1
    except KeyboardInterrupt:
        break

stream.disconnect()
