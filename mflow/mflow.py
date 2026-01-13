from datetime import time
from time import sleep

import zmq

from mflow.handlers import raw_1_0
from mflow.handlers import dseries_end_1_0
from mflow.handlers import dimage_1_0
from mflow.handlers import array_1_0
from mflow.handlers import dheader_1_0
from mflow.tools import SocketEventListener, ConnectionCountMonitor, no_clients_timeout_notifier

try:
    import ujson as json
except:
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

receive_handlers = {
    "array-1.0": array_1_0.Handler.receive,
    "dheader-1.0": dheader_1_0.Handler.receive,
    "dimage-1.0": dimage_1_0.Handler.receive,
    "dseries_end-1.0": dseries_end_1_0.Handler.receive,
    "raw-1.0": raw_1_0.Handler.receive
}

send_handlers = {
    "array-1.0": array_1_0.Handler.send,
    "dheade-1.0": dheader_1_0.Handler.send,
    "dimage-1.0": dimage_1_0.Handler.send,
    "dseries_end-1.0": dseries_end_1_0.Handler.send,
    "raw-1.0": raw_1_0.Handler.send
}


class Stream(object):

    def __init__(self):

        self.context = None
        self.socket = None
        self.address = None

        self.receiver = None

        self._socket_monitors = []
        self._socket_event_listener = SocketEventListener(self._socket_monitors)

    def connect(self, address, conn_type=CONNECT, mode=PULL, receive_timeout=None, queue_size=100, linger=1000,
                context=None, copy=True, send_timeout=None):
        """
        :param address:         Address to connect to, in the form of protocol://IP_or_Hostname:port, e.g.: tcp://127.0.0.1:40000
        :param conn_type:       Connection type - connect or bind to socket
        :param mode:            Message delivery mode PUSH/PULL PUB/SUB
        :param receive_timeout: Receive timeout in milliseconds (-1 = infinite)
        :param queue_size:      Queue size
        :param linger:          Linger option -i.e. how long to keep message in memory at socket shutdown - in milliseconds (-1 infinite)
        :param copy:            If False, allows to do zero-copy send and receive. It automatically sets the 0MQ track parameter to True
        :param send_timeout:    Send timeout in milliseconds (-1 = infinite)
        :return:
        """

        if not context:
            self.context = zmq.Context()
            self._context_is_owned = True
        else:
            self.context = context
            self._context_is_owned = False
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

            # Start the socket event listener, if there are monitors registered.
            if self._socket_monitors:
                self._socket_event_listener.start(self.socket)

        except Exception:
            logger.error("Full error: %s" % sys.exc_info()[1])
            raise RuntimeError("Unable to connect/bind to %s" % address)

        if receive_timeout:
            self.socket.RCVTIMEO = receive_timeout
            logger.info("Receive timeout set: %f" % receive_timeout)

        if send_timeout:
            self.socket.SNDTIMEO = send_timeout
            logger.info("Send timeout set: %f" % send_timeout)

        self.address = address
        self.zmq_copy = copy
        self.zmq_track = not copy

        # If socket is used for receiving messages, create receive handler
        if mode == zmq.SUB or mode == zmq.PULL:
            self.receiver = ReceiveHandler(self.socket, copy=copy)

    def register_socket_monitor(self, monitor):
        """
        Register a new connection monitor.
        :param monitor: Function to be called back at socket event.
        """
        if monitor not in self._socket_monitors:
            self._socket_monitors.append(monitor)

        # If the socket event listener is not running yet, but the socket is already connected, start it.
        if not self._socket_event_listener.monitor_listening.is_set() and (self.socket and not self.socket.closed):
            self._socket_event_listener.start(self.socket)

    def remove_socket_monitor(self, callback_function):
        """
        Remove connection monitor from this socket.
        :param callback_function: Callback function to remove.
        """
        if callback_function in self._socket_monitors:
            self._socket_monitors.remove(callback_function)

        # Stop the event listener if there are no more monitors.
        if not self._socket_monitors:
            self._socket_event_listener.stop()

    def disconnect(self):

        if self.socket.closed:
            logger.warning("Trying to close an already closed socket... ignore and return")
            return
        try:
            # Stop the socket event listener.
            self._socket_event_listener.stop()

            # Even if disconnect fails, we need to close.
            try:
                self.socket.disconnect(self.address)
            finally:
                self.socket.close()
                if self._context_is_owned:
                    self.context.term()

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

        message = None
        # Set blocking flag in receiver
        self.receiver.block = block
        receive_is_successful = False

        if not handler:
            try:
                # Dynamically select handler
                htype = self.receiver.header()["htype"]
            except zmq.Again:
                # not clear if this is needed
                self.receiver.flush(receive_is_successful)
                return message
            except KeyboardInterrupt:
                raise
            except:
                logger.exception('Unable to read header - skipping')
                # Clear remaining sub-messages if exist
                self.receiver.flush(receive_is_successful)
                return message

            try:
                handler = receive_handlers[htype]
            except:
                logger.debug(sys.exc_info()[1])
                logger.warning('htype - ' + htype + ' -  not supported')

        try:
            data = handler(self.receiver)
            # as an extra safety margin
            if data:
                receive_is_successful = True
                message = Message(self.receiver.statistics, data)
        except KeyboardInterrupt:
            raise
        except:
            logger.exception('Unable to decode message - skipping')

        # Clear remaining sub-messages if exist
        self.receiver.flush(receive_is_successful)

        return message

    def receive_raw(self, block=True):
        message = self.receive(handler=receive_handlers["raw-1.0"], block=block)
        return message

    def send(self, message, send_more=False, block=True, as_json=False):

        flags = 0
        if send_more:
            flags = zmq.SNDMORE
        if not block:
            flags = flags | zmq.NOBLOCK

        try:
            if as_json:
                self.socket.send_json(message, flags)
            else:
                self.socket.send(message, flags, copy=self.zmq_copy, track=self.zmq_track)
        except zmq.Again as e:
            if not block:
                pass
            else:
                raise e
        except zmq.ZMQError as e:
            logger.error(sys.exc_info()[1])
            raise e

    def forward(self, message, handler=None, block=True):

        if not handler:
            try:
                # Dynamically select handler
                htype = message["header"]["htype"]
            except Exception as e:
                logger.debug(sys.exc_info())
                logger.warning('Unable to read header - skipping')
                # Clear remaining sub-messages if exist
                raise e

            try:
                handler = send_handlers[htype]
            except:
                logger.debug(sys.exc_info()[1])
                logger.warning('htype - ' + htype + ' -  not supported')

        try:
            handler(message, send=self.send, block=block)
        except KeyboardInterrupt:
            raise
        except:
            logger.debug(str(sys.exc_info()[0]) + str(sys.exc_info()[1]))
            logger.warning('Unable to send message - skipping')


class ReceiveHandler:

    def __init__(self, socket, copy=True):
        self.socket = socket

        # Basic statistics
        self.statistics = Statistics()
        self.raw_header = None
        self.block = True

        self.zmq_copy = copy
        self.zmq_track = not copy

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
                raw = self.socket.recv(flags=flags, copy=self.zmq_copy, track=self.zmq_track)

            self.statistics.bytes_received += len(raw)
            if as_json:
                # non-copying recv returns a Frame object
                # use Frame.bytes field will incur a copy, but without causing
                # significant overhead since json header is of small size
                if isinstance(raw, zmq.Frame):
                    raw = raw.bytes
                return json.loads(raw.decode("utf-8"))
            else:
                # non-copying recv returns a Frame object
                # use Frame.buffer interface (read-only) to avoid extra copying
                if isinstance(raw, zmq.Frame):
                    raw = raw.buffer
                return raw
        except zmq.ZMQError:
            return None

    def flush(self, success=True):
        flags = 0 if self.block else zmq.NOBLOCK
        # Clear remaining sub-messages
        while self.has_more():
            try:
                self.socket.recv(flags=flags, copy=self.zmq_copy, track=self.zmq_track)
                logger.info('Skipping sub-message')
            except zmq.ZMQError:
                pass

        if success:
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


def connect(address, conn_type="connect", mode=zmq.PULL, queue_size=100, receive_timeout=None, linger=1000,
            no_client_action=None, no_client_timeout=10, copy=True, send_timeout=None):
    stream = Stream()

    # If no client action is specified, start monitor.
    if no_client_action:
        stream.register_socket_monitor(ConnectionCountMonitor(no_clients_timeout_notifier(no_client_action,
                                                                                          no_client_timeout)))

    stream.connect(address, conn_type=conn_type, mode=mode, receive_timeout=receive_timeout, queue_size=queue_size,
                   linger=linger, copy=copy, send_timeout=send_timeout)
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
