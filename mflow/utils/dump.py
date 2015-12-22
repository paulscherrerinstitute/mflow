import os
import mflow
import signal

counter = 0
folder = None


def dump(receiver):

    cnt = 0
    with open('{}/{}_{}.raw'.format(folder, '%06d' % counter, '%03d' % cnt), 'wb') as f:
        f.write(receiver.next())
    cnt += 1

    while receiver.has_more():
        with open('{}/{}_{}.raw'.format(folder, '%06d' % counter, '%03d' % cnt), 'wb') as f:
            f.write(receiver.next())
        cnt += 1


# def stop(*argv):
#     global receive_more
#     receive_more = False


def main():

    import argparse

    global counter
    global folder

    parser = argparse.ArgumentParser(description='Stream dump utility')

    parser.add_argument('source', type=str, help='Source address - format "tcp://<address>:<port>"')
    parser.add_argument('folder', type=str, help='Destination folder')

    arguments = parser.parse_args()

    folder = arguments.folder
    address = arguments.source

    if not os.path.exists(folder):
        os.makedirs(folder)

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
        stream.receive(handler=dump)
        counter += 1


if __name__ == '__main__':
    main()
