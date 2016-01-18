import mflow
import numpy
import argparse


def main():

    parser = argparse.ArgumentParser(description='Stream generation utility')

    parser.add_argument('-a', '--address', default="tcp://*:9999", type=str,
                        help='Address - format "tcp://<address>:<port>"')
    parser.add_argument('-s', '--size', default=1000, type=int,
                        help='Size of data to send - number of floats"')

    arguments = parser.parse_args()
    address = arguments.address
    size = arguments.size

    stream = mflow.connect(address, conn_type="bind", mode=mflow.PUSH)

    while True:
        stream.send(numpy.random.random(size), send_more=False)


if __name__ == '__main__':
    main()
