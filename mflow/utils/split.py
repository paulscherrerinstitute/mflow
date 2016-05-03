import mflow
import signal


class Splitter:

    def __init__(self, output_streams):
        self.output_streams = output_streams

    def receive(self, receiver):

        while True:
            message = receiver.next()
            more = receiver.has_more()

            for stream in self.output_streams:
                stream.send(message, send_more=more)

            if not more:
                break


def main():

    import argparse
    import sys

    parser = argparse.ArgumentParser(description='Stream dump utility')

    parser.add_argument('-c', '--config', help='Configuration file')

    parser.add_argument('source', type=str, nargs='?', help='Source address - format "tcp://<address>:<port>"')
    parser.add_argument('streams', type=str, nargs='*', help='Streams to generate - "tcp://<address>:<port>"')

    arguments = parser.parse_args()

    if arguments.config:
        print('config')
        (input_stream, splitter) = load_configuration(arguments.config)
    elif arguments.source and arguments.streams:
        streams_to_generate = arguments.streams
        address = arguments.source

        output_streams = []
        for new_stream in streams_to_generate:
            output_streams.append(mflow.connect(new_stream, conn_type=mflow.BIND, mode=mflow.PUSH))

        splitter = Splitter(output_streams)
        input_stream = mflow.connect(address)
    else:
        parser.print_help()
        sys.exit(-1)

    # Info: By here splitter and input_stream needs to be specified

    # Signal handling
    global receive_more
    receive_more = True

    def stop(*arguments):
        global receive_more
        receive_more = False
        signal.siginterrupt()

    signal.signal(signal.SIGINT, stop)

    while receive_more:
        input_stream.receive(handler=splitter.receive)


def load_configuration(filename):
    """
    Read in a configuration file like this:
    {
        "source": {
            "address": "tcp://localhost:7777",
            "mode": "PULL"
        },
        "streams": [
            {
                "address": "tcp://*:8888",
                "mode": "PUSH"
            }
        ]
    }
    """

    import json
    import re

    # Load configuration file
    with open(filename) as file_handle:
        configuration = json.load(file_handle)

    # Construct stream source
    address = configuration['source']['address']
    if re.match('tcp://\\*:.*', address):
        connection_type = mflow.BIND
    else:
        connection_type = mflow.CONNECT
    mode = mflow.PULL
    if 'mode' in configuration['source']:
        if configuration['source']['mode'].lower() == 'pull':
            mode = mflow.PULL
        elif configuration['source']['mode'].lower() == 'sub':
            mode = mflow.SUB
        else:
            raise Exception('Unsupported mode [%s] for source [%s]' % (configuration['source']['mode'], configuration['source']))
    print(connection_type)
    input_stream = mflow.connect(address, mode=mode, conn_type=connection_type)

    # Construct output streams
    output_streams = []
    for stream in configuration['streams']:
        address = stream['address']
        if re.match('tcp://\\*:.*', address):
            connection_type = mflow.BIND
        else:
            connection_type = mflow.CONNECT

        mode = mflow.PUSH
        if 'mode' in stream:
            if stream['mode'].lower() == 'push':
                mode = mflow.PUSH
            elif stream['mode'].lower() == 'pub':
                mode = mflow.PUB
            else:
                raise Exception('Unsupported mode [%s] for stream [%s]' % (stream['mode'], stream))

        output_streams.append(mflow.connect(address, conn_type=connection_type, mode=mode))

    return input_stream, Splitter(output_streams)


if __name__ == '__main__':
    main()
