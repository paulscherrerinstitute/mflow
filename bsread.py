import zmq
import json
import hashlib
import struct
import time
import logging
from collections import namedtuple


# Logger configuration
logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(name)s - %(message)s')

BSDataChannel = namedtuple('BSDataChannel', ['name', 'val', 'timestamp', 'pulseid'])


class Bsread(object):

    def __init__(self, mode=zmq.PULL):
        """
        Initialize ZMQ
        mode: zmq.PULL (default)
        """

        self.context = zmq.Context()
        self.socket = self.context.socket(mode)
        if mode == zmq.SUB:
            self.socket.setsockopt(zmq.SUBSCRIBE, '')

        self.address = None
        self.data_header = None
        self.header_hash = None
        self.header_type = None
        self.receive_  = None
        self.received_b = 0  # Size of received payload in bytes

    def connect(self, address="tcp://127.0.0.1:9999", conn_type="connect", timeout=None, queue_size=4):
        """
        Establish ZMQ connection
        timeout: connection timeout (default: 1000)
        address: address to connect to (default: "tcp://localhost:9999")
        conn_type: the type of connection, "connect" (default) or "bind"
        queue_size: size of 0MQ receiving queue (default: 4)
        """

        logger.info("Connecting to " + address)
        self.socket.set_hwm(queue_size)
        try:
            if conn_type == "connect":
                self.socket.connect(address)
            else:
                self.socket.bind(address)
        except:
            logger.error("Unable to connect to server. Hint: check IP address")

        if timeout:
            self.socket.RCVTIMEO = timeout

        logger.info("Connection done")
        self.address = address

    def disconnect(self):
        """Disconnects and close connection"""
        if self.socket.closed:
            logger.warn("trying to close an already closed socket... ignoring this command")
            return
        try:
            self.socket.disconnect(self.address)
            self.socket.close()
            logger.info("Disconnected")
        except:
            logger.info("Unable to disconnect properly")

    def receive(self):
        """
        Receive a message
        :return: map holding the data, timestamp, data and main header
        """

        header = self.socket.recv_json()

        if not self.receive_handler:
            # There is currently no receive handler defined, try to create one based on htype information
            import handler
            self.header_type = header['htype']
            self.receive_handler = handler.load(self.header_type)

        if self.header_type != header['htype']:
            raise RuntimeError('htype changed')

        return self.receive_handler.receive(self.socket, header)

    def recive_message(self):
        """ 
        Receive message in form of a dict holding 
        bsdata channels (as named tuples)
        """

        header = self.socket.recv_json()

        if not self.receive_handler:
            # There is currently no receive handler defined, try to create one based on htype information
            import handler
            self.header_type = header['htype']
            self.receive_handler = handler.load(self.header_type)

        if self.header_type != header['htype']:
            raise RuntimeError('htype changed')

        data = self.receive_handler.receive(self.socket, header)

        if 'data_header' in data:
            self.data_header = data['data_header']

        message = {}

        i=0;
        for channel in self.data_header['channels']:
            name = channel['name']
            val = data['data'][i]
            timestamp = data['timestamp'][i]
            message[name]=BSDataChannel(name,val,timestamp,header['pulse_id'])
            i=i+1


        self.received_b = self.received_b + data['size']


        return message



    def send(self):
        """
        Send test data
        """

        import time

        main_header = dict()
        main_header['htype'] = "bsr_m-1.0"

        data_header = dict()
        data_header['htype'] = "bsr_d-1.0"
        channels = []
        for index in range(0, 4):
            channel = dict()
            channel['encoding'] = "little"
            channel['name'] = "CHANNEL-%d" % index
            channel['type'] = "double"
            channels.append(channel)

        channel = dict()
        channel['encoding'] = "little"
        channel['name'] = "CHANNEL-STRING"
        channel['type'] = "string"
        channels.append(channel)

        channel = dict()
        channel['encoding'] = "little"
        channel['name'] = "CHANNEL-ARRAY_1"
        channel['type'] = "integer"
        channel['shape'] = [2]
        channels.append(channel)

        data_header['channels'] = channels

        data_header_json = json.dumps(data_header)

        main_header['hash'] = hashlib.md5(data_header_json).hexdigest()

        pulse_id = 0
        value = 0.0

        while True:
            current_timestamp = int(time.time())  # current timestamp in seconds
            main_header['pulse_id'] = pulse_id
            main_header['global_timestamp'] = {"epoch": current_timestamp, "ns": pulse_id}
            self.socket.send_json(main_header, zmq.SNDMORE)  # Main header
            self.socket.send_string(data_header_json, zmq.SNDMORE)  # Data header
            for index in range(0, 4):
                self.socket.send(struct.pack('d', value), zmq.SNDMORE)  # Data
                self.socket.send(struct.pack('q', current_timestamp) + struct.pack('q', 0), zmq.SNDMORE)  # Timestamp
                value += 0.1

            self.socket.send("hello-%d" % value, zmq.SNDMORE)  # Data
            self.socket.send(struct.pack('q', current_timestamp) + struct.pack('q', 0), zmq.SNDMORE)  # Timestamp
            msg = bytearray()
            msg.extend(struct.pack('i', pulse_id))
            msg.extend(struct.pack('i', pulse_id+1000))

            self.socket.send(msg, zmq.SNDMORE)  # Data
            self.socket.send(struct.pack('q', current_timestamp) + struct.pack('q', 0), zmq.SNDMORE)  # Timestamp

            self.socket.send('')
            pulse_id += 1
            # Send out every 10ms
            time.sleep(0.01)

