import json

from mflow.tools import ThroughputStatisticsPrinter

import mflow
import argparse


def main():

    parser = argparse.ArgumentParser(description="Stream generation utility")

    parser.add_argument("-a", "--address", default="tcp://*:9999", type=str,
                        help='Address - format "tcp://<address>:<port>"')
    parser.add_argument("-s", "--size", default=1, type=float,
                        help='Size of data to send (MB)"')
    parser.add_argument("-m", "--mode", default="push", type=str,
                        help="Communication mode - either push (default) or pub")

    arguments = parser.parse_args()
    address = arguments.address
    size = arguments.size
    mode = mflow.PUB if arguments.mode == "pub" else mflow.PUSH

    stream = mflow.connect(address, conn_type="bind", mode=mode)

    size_bytes = int(size*1024.0*1024.0)
    header = {"htype": "raw-1.0", "type": "int32", "shape": [1, size_bytes/4]}
    data = bytearray(size_bytes)

    print("Sending messages of size %f MB" % size)

    statistics = ThroughputStatisticsPrinter()
    message_statistics = argparse.Namespace(total_bytes_received=0)

    counter = 0
    try:
        while True:
            # Sending random data
            # stream.send(os.urandom(size * 8), send_more=False)  # Slow - cryptographically strong random numbers
            # stream.send(numpy.random.random(size), send_more=False)  # Fast - random numbers

            header["frame"] = counter
            stream.send(json.dumps(header).encode("utf-8"), send_more=True)
            stream.send(data, send_more=False)
            counter += 1

            # Not totally correct - the total bytes do not take into account the header.
            message_statistics.total_bytes_received += size_bytes
            message_statistics.messages_received = counter
            statistics.save_statistics(message_statistics)
    except KeyboardInterrupt:
        print("Terminated by user.")
        statistics.close()


if __name__ == "__main__":
    main()
