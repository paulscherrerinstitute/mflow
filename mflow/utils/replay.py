import os
from queue import Queue, Empty, Full
from threading import Thread, Event

import mflow
from os import listdir
from os.path import isfile, join
import argparse

# Timeout in seconds for queue operations.
QUEUE_TIMEOUT = 0.5


def main():
    parser = argparse.ArgumentParser(description='Stream replay utility')

    parser.add_argument('folder', type=str, help='Destination folder')
    parser.add_argument('-a', '--address', default="tcp://*:9999", type=str,
                        help='Address - format "tcp://<address>:<port>" (default: "tcp://*:9999")')
    parser.add_argument('-m', '--mode', default='push', type=str,
                        help='Communication mode - either push (default) or pub')

    arguments = parser.parse_args()

    folder = arguments.folder
    address = arguments.address
    mode = mflow.PUB if arguments.mode == 'pub' else mflow.PUSH

    if not os.path.exists(folder):
        os.makedirs(folder)

    data_queue = Queue(maxsize=16)
    stop_event = Event()

    reader_thread = Thread(target=reader_function, args=(folder, stop_event, data_queue))
    reader_thread.start()

    try:
        stream = mflow.connect(address, conn_type="bind", mode=mode)
        # If the reader thread is alive we still have hope to get something in the queue.
        while reader_thread.is_alive() or not data_queue.empty():
            try:
                data = data_queue.get(timeout=QUEUE_TIMEOUT)
                stream.send(data[BYTES], send_more=data[SEND_MORE])
                print('Sending %s [%s]' % (data[FILE_NAME], data[SEND_MORE]))
            except Empty:
                pass

    except KeyboardInterrupt:
        stop_event.set()
        reader_thread.join()
        print("Terminated by user.")
        exit(0)

# Tuple positional constants.
FILE_NAME = 0
BYTES = 1
SEND_MORE = 2


def reader_function(folder, stop_event, data_queue):
    files = sorted(listdir(folder))

    for index, raw_file in enumerate(files):
        # Stop reading files once the stop event is set.
        if stop_event.is_set():
            return

        filename = join(folder, raw_file)
        if not (raw_file.endswith('.raw') and isfile(filename)):
            continue

        with open(filename, mode='rb') as file_handle:
            send_more = False
            if index + 1 < len(files):  # Ensure that we don't run out of bounds
                send_more = raw_file.split('_')[0] == files[index + 1].split('_')[0]

            data = file_handle.read()

            # Wait until there is space in the queue.
            while True:
                try:
                    data_queue.put((raw_file, data, send_more), timeout=QUEUE_TIMEOUT)
                except Full:
                    # Stop waiting for the queue if the stop event is set.
                    if stop_event.is_set():
                        return
                # Data is in the queue.
                else:
                    break

if __name__ == '__main__':
    main()
