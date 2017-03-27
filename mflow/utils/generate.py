import json
import mflow
import argparse


def main():

    parser = argparse.ArgumentParser(description='Stream generation utility')

    parser.add_argument('-a', '--address', default="tcp://*:9999", type=str,
                        help='Address - format "tcp://<address>:<port>"')
    parser.add_argument('-s', '--size', default=1, type=float,
                        help='Size of data to send (MB)"')
    parser.add_argument('-m', '--mode', default='push', type=str,
                        help='Communication mode - either push (default) or pub')

    arguments = parser.parse_args()
    address = arguments.address
    size = arguments.size
    mode = mflow.PUB if arguments.mode == 'pub' else mflow.PUSH

    stream = mflow.connect(address, conn_type="bind", mode=mode)

    header = {'htype': 'raw-1.0', 'type': 'int32', 'shape': [0, 0]}
    size_bytes = int(size*1024.0*1024.0)
    data = bytearray(size_bytes)
    # message_size_mb = (size*8)/1024.0/1024.0

    print('Sending messages of size %f MB' % size)

    counter = 0
    while True:
        # Sending random data
        # stream.send(os.urandom(size * 8), send_more=False)  # Slow - cryptographically strong random numbers
        # stream.send(numpy.random.random(size), send_more=False)  # Fast - random numbers
        header["frame"] = counter
        stream.send(json.dumps(header).encode('utf-8'), send_more=True)
        stream.send(data, send_more=False)
        counter += 1


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("Terminated by user.")