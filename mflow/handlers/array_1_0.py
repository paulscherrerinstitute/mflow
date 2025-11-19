import json

import numpy


class Handler:

    @staticmethod
    def receive(receiver):

        header = receiver.next(as_json=True)

        return_value = None
        data = []

        # header contains: "htype", "shape", "type", "frame", "endianness", "source", "encoding", "tags"

        # Receiving data
        while receiver.has_more():
            raw_data = receiver.next()
            if raw_data:
                data.append(get_image(raw_data, header["type"], header["shape"]))
            else:
                data.append(None)

        if header or data:
            return_value = {"header": header,
                            "data": data}

        return return_value

    @staticmethod
    def send(message, send, block=True):
        send(json.dumps(message["header"]).encode(), send_more=True, block=True)

        # Get the number of data segments.
        number_of_segments = len(message["data"])

        # Forward all data segments available.
        for segment_index in range(number_of_segments):
            data_segment = message["data"][segment_index]
            more_blocks_to_send = segment_index + 1 != number_of_segments
            # Get the bytes for the data if the data is not already in bytes.
            if not isinstance(data_segment, bytes):
                data_segment = data_segment.tobytes()

            send(data_segment, block=block, send_more=more_blocks_to_send)


def get_image(raw_data, dtype, shape):
    return numpy.frombuffer(raw_data, dtype=dtype).reshape(shape)



