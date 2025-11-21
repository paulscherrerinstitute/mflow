import json


class Handler:

    @staticmethod
    def receive(receiver):
        header = receiver.next(as_json=True)

        data = []
        while receiver.has_more():
            segment = receiver.next() or None
            data.append(segment)

        res = None
        if header or data:
            res = {
                "header": header,
                "data": data
            }

        return res


    @staticmethod
    def send(message, send, block=True):
        send(json.dumps(message["header"]).encode(), send_more=True, block=True)

        for segment in message["data"]:
            send(segment, block=block)



