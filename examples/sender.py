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
import json

logger = logging.getLogger("mflow.mflow")
logger.setLevel(logging.ERROR)

address = "tcp://127.0.0.1:40000"

stream = mflow.connect(address, conn_type=mflow.BIND, mode=mflow.PUSH, receive_timeout=1, queue_size=1)

data = np.ones(10, dtype=np.int32)
i = 0
while i < 16:
    try:

        header = {"htype": "array-1.0", "type": "int32", "shape": [10, ], "frame": i}
        stream.send(json.dumps(header).encode('utf-8'), send_more=True, block=False)
        stream.send(data.tobytes(), block=True)
        i += 1

        print("Sending message %d" % i)
        # Send out every 10ms
        time.sleep(0.01)

    except KeyboardInterrupt:
        break

stream.disconnect()
