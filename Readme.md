[![Build Status](https://travis-ci.org/paulscherrerinstitute/mflow.svg?branch=master)](https://travis-ci.org/paulscherrerinstitute/mflow) [![Build status](https://ci.appveyor.com/api/projects/status/tas7986wilnm4090?svg=true)](https://ci.appveyor.com/project/simongregorebner/mflow)

# Overview
__mflow__ facilitates the handling of ZMQ data streams. It provides basic accounting and statistics on messages
received/send as well as an easy way to handle different types of messages within a stream.

Right now mflow comes with following message type support:
* array-1.0
* bsr_m-1.0
* dheader-1.0 (Dectris Eiger)
* dimage-1.0 (Dectris Eiger)
* dseries_end-1.0 (Dectris Eiger)

# Installation
## Pip
The mflow package is available on [https://pypi.python.org](https://pypi.python.org/pypi/mflow) and can be installed via pip

```bash
pip install mflow
```

## Anaconda

The mflow package is available on [anaconda.org](https://anaconda.org/paulscherrerinstitute/mflow) and can be installed as follows:

```bash
conda install -c https://conda.anaconda.org/paulscherrerinstitute mflow
```

# Usage

Connect/Create stream:

```python
stream = mflow.connect(address, conn_type=mflow.CONNECT, mode=mflow.PULL, receive_timeout=None, queue_size=100)
```

Receive a message:

```python
message = stream.receive(self, handler=None)
```

The returned `message` object contains the current receiving  statistics in `message.statistics` and the actual
message data in `message.data`.

If there should be no dynamic resolution of the message handler an explicit handler can be specified to handle the
incoming message.


Disconnecting stream:

```python
stream.disconnect()
```


Sending message (ensure that you specified the correct mode!):

```python
stream.send('message content', send_more=True)
```


Register multiple custom (htype) handlers:

```python
def receive_function(receiver):
      header = receiver.next(as_json=True)

      data = []
      while receiver.has_more():
          segment = receiver.next() or None
          data.append(segment)

      res = {
          "header": header,
          "data": data
      }
      return res


my_handlers = dict()
my_handlers['my_htype-1.0'] = receive_function
# ... register more handlers ...

# set handlers
stream.handlers = my_handlers
```

__Note:__ Handlers need to be registered before calling `receive()`.

Example:

```python
import mflow
stream = mflow.connect('tcp://sf-lc:9999')

# Receive "loop"
message  = stream.receive()
print(message.statistics.messages_received)

stream.disconnect()
```

## Advanced

### Register Additional Handlers
Manually register more handlers that are not provided by this package (after creating the stream)

```python
stream.handlers['id'] = myhandler
```

### Merge Streams
mflow provides a simple class to merge two ore more streams. The default implementation merges the messages round robin, i.e. you will receive message 1 from stream 1 then message 1 from stream 2, then message 2 from stream 1 ...

```
import mflow
stream_one = mflow.connect('tcp://source1:7777')
stream_two = mflow.connect('tcp://source2:7779')

import mflow.utils
stream = mflow.utils.Merge(stream_one, stream_two)

message = stream.receive()

stream.disconnect()
```

# Command Line
The Anaconda mflow package comes with several command line tools useful for testing streaming.

## m_stats
Show statistics for incoming streams. Useful for measure the maximum throughput for a given stream on a link.

```bash
usage: m_stats [-h] source

Stream statistic utility

positional arguments:
  source      Source address - format "tcp://<address>:<port>"

optional arguments:
  -h, --help  show this help message and exit
```

## m_generate
Generate a random stream. This is useful, together with `m_stats` to measure possible throughput.

```bash
usage: m_generate [-h] [-a ADDRESS] [-s SIZE]

Stream generation utility

optional arguments:
  -h, --help            show this help message and exit
  -a ADDRESS, --address ADDRESS
                        Address - format "tcp://<address>:<port>"
  -s SIZE, --size SIZE  Size of data to send (MB)"
```

## m_dump
Dump an incoming stream to disk or screen. While dumping into files, `m_dump` saves all sub-messages into individual files.
The option `-s` can be used if you are only interested in the first n submessages (e.g. header)

```bash
usage: m_dump [-h] [-s SKIP] source [folder]

Stream dump utility

positional arguments:
  source                Source address - format "tcp://<address>:<port>"
  folder                Destination folder

optional arguments:
  -h, --help            show this help message and exit
  -s SKIP, --skip SKIP  Skip sub-messages starting from this number (including
                        number)
```

## m_replay
Replay a recorded (via m_dump) stream.

```bash
usage: m_replay [-h] [-a ADDRESS] folder

Stream replay utility

positional arguments:
  folder                Destination folder

optional arguments:
  -h, --help            show this help message and exit
  -a ADDRESS, --address ADDRESS
                        Address - format "tcp://<address>:<port>" (default:
                        "tcp://*:9999")
```

## m_split
Split an incoming stream into multiple streams. Currently only the PUSH/PULL scheme is supported.

```bash
usage: m_split [-h] [-c CONFIG] [source] [streams [streams ...]]

Stream dump utility

positional arguments:
  source                Source address - format "tcp://<address>:<port>"
  streams               Streams to generate - "tcp://<address>:<port>"

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Configuration file
```

The -c / --config option accepts a configuration file as follows:
```json
{
    "source": {
        "address": "tcp://localhost:7777",
        "mode": "PULL",
        "queue_size": 10
    },
    "streams": [
        {
            "address": "tcp://*:8888",
            "mode": "PUSH"
        }
    ]
}
```

If an address is specified in the format of 'tcp://*:<port>' the splitter will do a bind on that address and opens the specified port. If there is a hostname given, the splitter tries to connect to the address.
Supported modes are PULL/SUB for the source and PUSH/PUB for outgoing streams.

The default value for mode (if omitted) is PULL for the source and PUSH for output streams. The default queue size (if omitted) is 100 for both source and output streams.

Output streams can be reduced by applying a modulo. This can be done by specifying the modulo attribute as follows:

```json
{
    "source": {
        "address": "tcp://localhost:7777",
        "mode": "PULL",
        "queue_size": 10
    },
    "streams": [
        {
            "address": "tcp://*:8888",
            "mode": "PUSH",
            "modulo": 1000
        }
    ]
}

Such a configuration will result in that only every 1000 message is send out to the output stream.


# Development

## PyPi
Upload package to pypi.python.org

```bash
python setup.py sdist upload
```

## Anaconda
To build the anaconda package do:

```bash
conda build conda_recipe
```

Afterwards the package can be uploaded to anaconda.org via

```bash
anaconda upload <path_to.tar.bz2_file>
```
