import mflow
import signal


class Splitter:

    def __init__(self, streams):
        self.streams = streams

    def receive(self, receiver):

        while True:
            message = receiver.next()
            more = receiver.has_more()

            for stream in self.streams:
                stream.send(message, send_more=more)

            if not more:
                break


def main():

    import argparse

    parser = argparse.ArgumentParser(description='Stream dump utility')

    parser.add_argument('source', type=str, help='Source address - format "tcp://<address>:<port>"')
    parser.add_argument('streams', type=str, nargs='+', help='Streams to generate - "tcp://<address>:<port>"')

    arguments = parser.parse_args()

    streams_to_generate = arguments.streams
    address = arguments.source

    streams = []
    for new_stream in streams_to_generate:
        streams.append(mflow.connect(new_stream, conn_type=mflow.BIND, mode=mflow.PUSH))

    splitter = Splitter(streams)

    stream = mflow.connect(address)

    # Signal handling
    global receive_more
    receive_more = True

    def stop(*arguments):
        global receive_more
        receive_more = False
        signal.siginterrupt()

    signal.signal(signal.SIGINT, stop)

    while receive_more:
        stream.receive(handler=splitter.receive)


if __name__ == '__main__':
    main()
