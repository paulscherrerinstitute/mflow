import argparse
import os
from os import listdir
from os.path import isfile, join

import mflow


def reply_folder(bind_address, folder, mode):

    if not os.path.exists(folder):
        raise ValueError("Specified folder '%s' does not exist.")

    stream = mflow.connect(bind_address, conn_type="bind", mode=mode)

    files = sorted(listdir(folder))

    for index, raw_file in enumerate(files):
        filename = join(folder, raw_file)
        if not (raw_file.endswith(".raw") and isfile(filename)):
            continue

        with open(filename, mode="rb") as file_handle:
            send_more = False
            if index + 1 < len(files):  # Ensure that we don't run out of bounds
                send_more = raw_file.split("_")[0] == files[index + 1].split("_")[0]

            print("Sending %s [%s]" % (raw_file, send_more))
            stream.send(file_handle.read(), send_more=send_more)


def main():

    parser = argparse.ArgumentParser(description="Stream replay utility")

    parser.add_argument("folder", type=str, help="Destination folder")
    parser.add_argument("-a", "--address", default="tcp://*:9999", type=str,
                        help='Address - format "tcp://<address>:<port>" (default: "tcp://*:9999")')
    parser.add_argument("-m", "--mode", default="push", type=str,
                        help="Communication mode - either push (default) or pub")

    arguments = parser.parse_args()

    folder = arguments.folder
    address = arguments.address
    mode = mflow.PUB if arguments.mode == "pub" else mflow.PUSH

    reply_folder(address, folder, mode)





if __name__ == "__main__":
    main()



