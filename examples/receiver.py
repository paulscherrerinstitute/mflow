import logging

import mflow


logger = logging.getLogger("mflow.mflow")
logger.setLevel(logging.ERROR)

address = "tcp://127.0.0.1:40000"

stream = mflow.connect(address, conn_type=mflow.CONNECT, mode=mflow.PULL, receive_timeout=1, queue_size=1)

while True:
    message = stream.receive()
    if message:
        print("Received frame %d." % message.data["header"]["frame"])
        print(message.data)
        print(message.statistics.messages_received)

stream.disconnect()



