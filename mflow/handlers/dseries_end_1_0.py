import json


class Handler:

    @staticmethod
    def receive(receiver):
        header = receiver.next(as_json=True)
        return {'header': header}

    @staticmethod
    def send(message, send, block=True):
        send(json.dumps(message["header"]).encode(), send_more=False, block=True)