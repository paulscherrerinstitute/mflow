import zmq


def main():

    dump_folder = ''
    mode = zmq.PUSH
    address = "tcp://*:9999"
    conn_type = "bind"

    import re

    sender = Sender(mode=mode)
    sender.connect(address=address, conn_type=conn_type)

    socket = sender.socket

    from os import listdir
    from os.path import isfile, join
    files = [raw_file for raw_file in listdir(dump_folder) if (isfile(join(dump_folder, raw_file)) and (raw_file.endswith('.raw') or raw_file.endswith('.json')))]

    files.sort(key=natural_key)
    # for raw_file in files:
    for index, raw_file in enumerate(files):
        with open(join(dump_folder, raw_file), mode='rb') as file_handle:

            # Check whether message number increases in the next file
            # If yes, don't send flag SNDMORE

            send_more = False
            if index+1 < len(files):  # Ensure that we don't run out of bounds
                numbers = re.findall(r'\d+', raw_file)
                numbers_next = re.findall(r'\d+', files[index+1])

                if numbers[0] == numbers_next[0]:
                    send_more = True

            content = file_handle.read()

            if send_more:
                socket.send(content, zmq.SNDMORE)
                logger.info(raw_file+' > send more')
            else:
                socket.send(content)
                logger.info(raw_file+' > send')

    # Wait 5 more seconds to give the client a chance to retrieve all messages from the
    # sender side queue
    import time
    time.sleep(2)

    sender.disconnect()


if __name__ == '__main__':
    main()
