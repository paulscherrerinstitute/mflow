import signal

import mflow


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


class FilterSplitter:

    def __init__(self, output_streams, output_filters):
        self.output_streams = output_streams
        self.output_filters = output_filters

    def receive(self, receiver):

        for filter in self.output_filters:
            if filter:
                filter.update()

        while True:
            message = receiver.next()
            more = receiver.has_more()

            for stream, filter in list(zip(self.output_streams, self.output_filters)):
                if filter:
                    if filter.check():
                        stream.send(message, send_more=more)
                else:
                    stream.send(message, send_more=more)

            if not more:
                break


class ModuloFilter:
    def __init__(self, modulo=1):
        self.modulo = modulo
        self.counter = 0  # Internal counter

    def update(self):
        self.counter += 1

    def check(self):
        if self.counter == self.modulo:
            self.counter = 0
            return True
        return False


def main():

    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Stream dump utility")

    parser.add_argument("-c", "--config", help="Configuration file")

    parser.add_argument("source", type=str, nargs="?", help='Source address - format "tcp://<address>:<port>"')
    parser.add_argument("streams", type=str, nargs="*", help='Streams to generate - "tcp://<address>:<port>"')

    arguments = parser.parse_args()

    if arguments.config:
        print("config")
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
            "mode": "PULL",
            "queue_size": 100
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
    address = configuration["source"]["address"]

    if re.match("tcp://\\*:.*", address):
        connection_type = mflow.BIND
    else:
        connection_type = mflow.CONNECT

    mode = mflow.PULL
    if "mode" in configuration["source"]:
        if configuration["source"]["mode"].lower() == "pull":
            mode = mflow.PULL
        elif configuration["source"]["mode"].lower() == "sub":
            mode = mflow.SUB
        else:
            raise Exception("Unsupported mode [%s] for source [%s]" % (configuration["source"]["mode"], configuration["source"]))

    queue_size = 100
    if "queue_size" in configuration["source"]:
        queue_size = configuration["source"]["queue_size"]

    input_stream = mflow.connect(address, mode=mode, conn_type=connection_type, queue_size=queue_size)

    # Construct output streams
    output_streams = []
    output_filters = []
    use_filter = False
    for stream in configuration["streams"]:
        address = stream["address"]

        if re.match("tcp://\\*:.*", address):
            connection_type = mflow.BIND
        else:
            connection_type = mflow.CONNECT

        mode = mflow.PUSH
        if "mode" in stream:
            if stream["mode"].lower() == "push":
                mode = mflow.PUSH
            elif stream["mode"].lower() == "pub":
                mode = mflow.PUB
            else:
                raise Exception("Unsupported mode [%s] for stream [%s]" % (stream["mode"], stream))

        queue_size = 100
        if "queue_size" in stream:
            queue_size = stream["queue_size"]

        if "modulo" in stream:
            output_filters.append(ModuloFilter(int(stream["modulo"])))
            use_filter = True
        else:
            output_filters.append(None)

        output_streams.append(mflow.connect(address, conn_type=connection_type, mode=mode, queue_size=queue_size))

    if use_filter:
        return input_stream, FilterSplitter(output_streams, output_filters)
    else:
        return input_stream, Splitter(output_streams)





if __name__ == "__main__":
    main()



