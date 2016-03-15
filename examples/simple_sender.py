import sys
import os

try:
    import mflow
except:
    sys.path.append(os.environ["PWD"] + "/../")
    import mflow
    
import logging
import numpy as np
import json


logger = logging.getLogger("mflow.mflow")
logger.setLevel(logging.DEBUG)

address = "tcp://127.0.0.1:40000"

stream = mflow.connect(address, conn_type=mflow.BIND, mode=mflow.PUSH, receive_timeout=1, queue_size=1000)

data = np.ones(10)
i = 0
while True:
    try:
        stream.send_message("array-1.0", data, frame=i)
        i += 1
    except KeyboardInterrupt:
        break

stream.disconnect()
