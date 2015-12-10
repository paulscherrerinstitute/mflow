import os
import stream

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

    dstream = stream.connect(address)

    while True:
        dstream.receive(handler=dump)
        counter += 1


if __name__ == '__main__':
    main()
