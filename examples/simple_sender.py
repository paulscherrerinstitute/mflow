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
md = dict(
    htype="array-1.0",
    type=str(data.dtype),
    shape=data.shape,
)

print md
while True:
    try:
        stream.send(json.dumps(md), send_more=True)
        stream.send(data, send_more=False)
    except KeyboardInterrupt:
        break

stream.disconnect()

