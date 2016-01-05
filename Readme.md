# Overview
__mflow__ facilitates the handling of ZMQ data streams. It provides basic accounting and statistics on messages 
received/send as well as an easy way to handle new types of data streams.
 

# Usage

## Connect to / Create a stream
```python
def connect(self, address, conn_type="connect", mode=zmq.PULL, receive_timeout=None, queue_size=100):
```

For connecting to a stream


## Example
```python
stream = mflow.connect('tcp://sf-lc:9999')
message  = stream.receive()
stream.disconnect()
```