import argparse
import os
import signal

import mflow


#TODO: globals?
counter = 0
folder = None
skip_from_message = None


def dump(receiver):
    cnt = 0
    message = receiver.next()
    cnt += 1

    if not skip_from_message or cnt < skip_from_message:
        with open("{}/{}_{}.raw".format(folder, "%06d" % counter, "%03d" % (cnt-1)), "wb") as f:
            f.write(message)

    while receiver.has_more():
        message = receiver.next()
        cnt += 1

        if not skip_from_message or cnt < skip_from_message:
            with open("{}/{}_{}.raw".format(folder, "%06d" % counter, "%03d" % (cnt-1)), "wb") as f:
                f.write(message)


def dump_screen(receiver):
    cnt = 0
    message = receiver.next()
    cnt += 1

    if not skip_from_message or cnt < skip_from_message:
        print(message)

    while receiver.has_more():
        message = receiver.next()
        cnt += 1

        if not skip_from_message or cnt < skip_from_message:
            print(message)


#def stop(*argv):
#    global receive_more
#    receive_more = False


def main():
    global counter
    global folder
    global skip_from_message

    parser = argparse.ArgumentParser(description="Stream dump utility")

    parser.add_argument("source", type=str, help='Source address - format "tcp://<address>:<port>"')
    parser.add_argument("folder", default=None, nargs="?", type=str, help="Destination folder")
    parser.add_argument("-m", "--mode", default="pull", type=str,
                        help="Communication mode - either pull (default) or sub")
    parser.add_argument("-s", "--skip", default=None, type=int,
                        help="Skip sub-messages starting from this number (including number)")

    arguments = parser.parse_args()

    folder = arguments.folder
    address = arguments.source
    skip_from_message = arguments.skip
    mode = mflow.SUB if arguments.mode == "sub" else mflow.PULL

    if folder and not os.path.exists(folder):
        os.makedirs(folder)

    stream = mflow.connect(address, mode=mode)

    # Signal handling
    global receive_more #TODO: is this correct?
    receive_more = True

    def stop(*args):
        global receive_more
        receive_more = False
        signal.siginterrupt()

    signal.signal(signal.SIGINT, stop)

    # Select handler
    handler = dump
    if not folder:
        handler = dump_screen

    while receive_more:
        stream.receive(handler=handler)
        counter += 1





if __name__ == "__main__":
    main()



