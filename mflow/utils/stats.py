import mflow
import signal

counter = 0
folder = None


def dump(receiver):
    receiver.next()
    while receiver.has_more():
        receiver.next()


def main():

    import time
    import argparse
    parser = argparse.ArgumentParser(description='Stream statistic utility')

    parser.add_argument('source', type=str, help='Source address - format "tcp://<address>:<port>"')

    arguments = parser.parse_args()

    address = arguments.source
    stream = mflow.connect(address)

    previous_time = 0

    # Signal handling
    global more
    more = True

    def stop(*arguments):
        global more
        more = False
        signal.siginterrupt()

    signal.signal(signal.SIGINT, stop)

    while more:
        message = stream.receive(handler=dump)

        now = time.time()

        # Print every second
        # TODO Need to be done differently as at the end of a stream the last stats do not show up
        # Use threading.Timer(1, foo).start()
        # (http://stackoverflow.com/questions/8600161/executing-periodic-actions-in-python)
        # As printing out every time a message is received will slow down the receive process
        if (now - previous_time) > 0.1:
            print(chr(27) + "[2J")
            print("_"*60)
            print('Messages received: {}'.format(message.statistics.messages_received))
            print('Total bytes received: {} Mb'.format(message.statistics.total_bytes_received/1024.0/1024.0))
            print("_"*60)
            print('')

            previous_time = now


if __name__ == '__main__':
    main()
