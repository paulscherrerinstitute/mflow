import zmq
import json
import logging
import sys

# setting up logging
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
#formatter = logging.Formatter("[%(name)s][%(levelname)s] %(message)s")
formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s')

ch.setFormatter(formatter)
logger.addHandler(ch)

CONNECT = 'connect'
BIND = 'bind'

PUB = zmq.PUB
SUB = zmq.SUB
PUSH = zmq.PUSH
PULL = zmq.PULL


class Stream(object):

    def __init__(self):

        self.context = None
        self.socket = None
        self.address = None

        self.receiver = None
        self.handlers = {}

    def connect(self, address, conn_type=CONNECT, mode=PULL, receive_timeout=None, queue_size=100, linger=1000):
        """
        :param address:         Address to connect to, in the form of protocol://IP_or_Hostname:port, e.g.: tcp://127.0.0.1:40000
        :param conn_type:       Connection type - connect or bind to socket
        :param mode:            Message delivery mode PUSH/PULL PUB/SUB
        :param receive_timeout: Receive timeout in milliseconds (-1 = infinite)
        :param queue_size:      Queue size
        :param linger:          Linger option -i.e. how long to keep message in memory at socket shutdown - in milliseconds (-1 infinite)
        :return:
        """

        self.context = zmq.Context()
        self.socket = self.context.socket(mode)
        if mode == zmq.SUB:
            self.socket.setsockopt_string(zmq.SUBSCRIBE, '')

        self.socket.setsockopt(zmq.LINGER, linger)
        self.socket.set_hwm(queue_size)
        try:
            if conn_type == CONNECT:
                self.socket.connect(address)
                logger.info("Connected to %s" % address)
            else:
                self.socket.bind(address)
                logger.info("Bound to %s" % address)
        except:
            logger.error("Unable to connect to %s. Hint: check IP address. It must be something like tcp://127.0.0.1:40000" % address)

        if receive_timeout:
            self.socket.RCVTIMEO = receive_timeout
            logger.info("Timeout set: ", receive_timeout )

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
            logger.debug(sys.exc_info()[1])
            logger.info("Unable to disconnect properly")

    def receive(self, handler=None, block=True):
        """
        :param handler:     Reference to a specific message handler function to use for interpreting
                            the message to be received
        :param block:       Blocking receive call
        :return:            Map holding the data, timestamp, data and main header
        """

        data = None
        # Set blocking flag in receiver
        self.receiver.block = block

        if not handler:
            try:
                # Dynamically select handler
                htype = self.receiver.header()["htype"]
            except zmq.Again:
                if not block:
                    return Message(self.receiver.statistics, data)
            except zmq.ZMQError:
                logger.debug(sys.exc_info())
                logger.warning('Unable to read header - skipping')
                # Clear remaining sub-messages if exist
                self.receiver.flush()
                return Message(self.receiver.statistics, data)

            try:
                handler = self.handlers[htype]
            except:
                logger.debug(sys.exc_info()[1])
                logger.warning('htype - ' + htype + ' -  not supported')

        try:
            data = handler(self.receiver)
            self.receiver.statistics.messages_received += 1
        except:
            logger.debug(sys.exc_info()[1])
            logger.warning('Unable to decode message - skipping')

        # Clear remaining sub-messages if exist
        self.receiver.flush()

        return Message(self.receiver.statistics, data)

    def send(self, message, send_more=False, block=True):
        flags = 0
        if send_more:
            flags = zmq.SNDMORE
        if not block:
            flags = flags | zmq.NOBLOCK

        try:
            self.socket.send(message, flags)
        except zmq.Again as e:
            if not block:
                pass
            else:
                raise e
        except zmq.ZMQError as e:
            logger.error(sys.exc_info()[1])
            raise e


class ReceiveHandler:

    def __init__(self, socket):
        self.socket = socket

        # Basic statistics
        self.statistics = Statistics()
        self.raw_header = None
        self.block = True

    def header(self):
        flags = 0 if self.block else zmq.NOBLOCK
        self.raw_header = self.socket.recv(flags=flags)
        return json.loads(self.raw_header.decode("utf-8"))

    def has_more(self):
        return self.socket.getsockopt(zmq.RCVMORE)

    def next(self, as_json=False):
        try:
            if self.raw_header:
                raw = self.raw_header
                self.raw_header = None
            else:
                flags = 0 if self.block else zmq.NOBLOCK
                raw = self.socket.recv(flags=flags)

            self.statistics.bytes_received += len(raw)
            if as_json:
                return json.loads(raw.decode("utf-8"))
            return raw
        except zmq.ZMQError:
            return None

    def flush(self):
        flags = 0 if self.block else zmq.NOBLOCK
        # Clear remaining sub-messages
        while self.has_more():
            try:
                self.socket.recv(flags=flags)
                logger.info('Skipping sub-message')
            except zmq.ZMQError:
                pass

        # Update statistics
        #self.statistics.total_bytes_received += self.statistics.bytes_received
        self.statistics.bytes_received = 0
        #self.statistics.messages_received += 1


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
        # super().__init__()
        self.blacklist = {}

    def __missing__(self, key):
        try:
            if key not in self.blacklist:
                logger.info('Handler missing - try to load handler for - '+key)
                module = __import__("mflow.handlers." + key.replace('.', '_').replace('-', '_'), fromlist=".")
                handler = module.Handler().receive
                self[key] = handler
                logger.info('Handler loaded')
                return handler
        except:
            logger.warning('Cannot load handler for key '+key+' - blacklisting')
            self.blacklist[key] = 1

        raise KeyError(key)


def connect(address, conn_type="connect", mode=zmq.PULL, queue_size=100, receive_timeout=None, linger=1000):
    stream = Stream()
    stream.handlers = DefaultHandlers()
    stream.connect(address, conn_type=conn_type, mode=mode, receive_timeout=receive_timeout, queue_size=queue_size, linger=linger)
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
