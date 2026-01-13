import json

import numpy


class Handler:

    @staticmethod
    def receive(receiver):
        # header contains: "htype", "shape", "type", "frame", "endianness", "source", "encoding", "tags"
        header = receiver.next(as_json=True)

        data = []
        while receiver.has_more():
            segment = receiver.next() or None
            if segment:
                segment = get_array(segment, header["type"], header["shape"])
            data.append(segment)

        res = None #TODO: this is inconsistent -- should it always be a dict?
        if header or data:
            res = {
                "header": header,
                "data": data
            }

        return res


    @staticmethod
    def send(message, send, block=True):
        send(json.dumps(message["header"]).encode(), send_more=True, block=True)

        data = message["data"]
        last_index = len(data) - 1

        for index, segment in enumerate(data):
            if not isinstance(segment, bytes):
                segment = segment.tobytes()

            send_more = index < last_index
            send(segment, block=block, send_more=send_more)



def get_array(raw_data, dtype, shape):
    return numpy.frombuffer(raw_data, dtype=dtype).reshape(shape)



