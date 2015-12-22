import zmq
import json
import logging

logger = logging.getLogger(__name__)


class Stream(object):

    def __init__(self):

        self.context = None
        self.socket = None
        self.address = None

        self.receiver = None
        self.handlers = {}

    def connect(self, address, conn_type="connect", mode=zmq.PULL, receive_timeout=None, queue_size=100):
        """
        :param address:         Address to connect to
        :param conn_type:       Connection type - connect or bind to socket
        :param mode:            Message delivery mode PUSH/PULL PUB/SUB
        :param receive_timeout: Receive timeout in milliseconds (-1 = infinite)
        :param queue_size:      Queue size
        :return:
        """

        self.context = zmq.Context()
        self.socket = self.context.socket(mode)
        if mode == zmq.SUB:
            self.socket.setsockopt(zmq.SUBSCRIBE, '')

        logger.info("Connecting to " + address)
        self.socket.set_hwm(queue_size)
        try:
            if conn_type == "connect":
                self.socket.connect(address)
            else:
                self.socket.bind(address)
        except:
            logger.error("Unable to connect to server. Hint: check IP address")

        if receive_timeout:
            self.socket.RCVTIMEO = receive_timeout

        logger.info("Connection done")
        self.address = address

        # If socket is used for receiving messages, create receive handler
        if mode == zmq.SUB or mode == zmq.PULL:
            self.receiver = ReceiveHandler(self.socket)

    def disconnect(self):

        if self.socket.closed:
            logger.warn("Trying to close an already closed socket... ignore and return")
            return
        try:
            self.socket.disconnect(self.address)
            self.socket.close()
            logger.info("Disconnected")
        except:
            logger.info("Unable to disconnect properly")

    def receive(self, handler=None):
        """
        :param handler:     Reference to a specific message handler function to use for interpreting
                            the message to be received
        :return:            Map holding the data, timestamp, data and main header
        """

        data = None
        if not handler:
            try:
                # Dynamically select handler
                htype = self.receiver.header()["htype"]
            except:
                logger.warning('Unable to read header')

            try:
                handler = self.handlers[htype]
            except:
                logger.warning('htype - '+htype+' -  not supported')

        try:
            data = handler(self.receiver)
        except:
            logger.warning('Unable to decode message - skipping')

        # Clear remaining sub-messages if exist
        self.receiver.flush()

        return Message(self.receiver.statistics, data)

    def send(self, message, send_more=True):
        if send_more:
            self.socket.send(message, zmq.SNDMORE)
        else:
            self.socket.send(message)


class ReceiveHandler:

    def __init__(self, socket):
        self.socket = socket

        # Basic statistics
        self.statistics = Statistics()
        self.raw_header = None

    def header(self):
        self.raw_header = self.socket.recv()
        return json.loads(self.raw_header.decode("utf-8"))

    def has_more(self):
        return self.socket.getsockopt(zmq.RCVMORE)

    def next(self, as_json=False):
        if self.raw_header:
            raw = self.raw_header
            self.raw_header = None
        else:
            raw = self.socket.recv()

        self.statistics.bytes_received += len(raw)
        if as_json:
            return json.loads(raw.decode("utf-8"))
        return raw

    def flush(self):
        # Clear remaining sub-messages
        while self.has_more():
            self.socket.recv()
            logger.info('Skipping sub-message')

        # Update statistics
        self.statistics.total_bytes_received += self.statistics.bytes_received
        self.statistics.bytes_received = 0
        self.statistics.messages_received += 1


class Statistics:
    def __init__(self):
        self.bytes_received = 0
        self.total_bytes_received = 0
        self.messages_received = 0


class Message:
    def __init__(self, statistics, data):
        self.statistics = statistics
        self.data = data


class DefaultHandlers(dict):
    def __init__(self):
        super().__init__()
        self.blacklist = {}

    def __missing__(self, key):
        try:
            if key not in self.blacklist:
                logger.info('Handler missing - try to load handler for - '+key)
                module = __import__("handlers." + key.replace('.', '_').replace('-', '_'), fromlist=".")
                handler = module.Handler().receive
                self[key] = handler
                logger.info('Handler loaded')
                return handler
        except:
            logger.warning('Cannot load handler for key '+key+' - blacklisting')
            self.blacklist[key] = 1

        raise KeyError(key)


def connect(address, conn_type="connect", mode=zmq.PULL, queue_size=100, receive_timeout=None):
    stream = Stream()
    stream.handlers = DefaultHandlers()
    stream.connect(address, conn_type=conn_type, mode=mode, receive_timeout=receive_timeout, queue_size=queue_size)
    return stream


def disconnect(stream):
    stream.disconnect()


def main():
    # Configuration logging
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(name)s - %(message)s')

    stream = connect('tcp://sf-lc:9999')
    while True:
        message = stream.receive()
        print('Messages received: %d' % message.statistics.messages_received)

    stream.disconnect()


if __name__ == '__main__':
    main()