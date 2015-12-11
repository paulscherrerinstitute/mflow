import mflow
import zmq


def connect(address, conn_type="connect", mode=zmq.PULL, queue_size=100, receive_timeout=None):
    stream = mflow.Stream()
    stream.handlers = mflow.DefaultHandlers()
    stream.connect(address, conn_type=conn_type, mode=mode, receive_timeout=receive_timeout, queue_size=queue_size)
    return stream


def disconnect(stream):
    stream.disconnect()