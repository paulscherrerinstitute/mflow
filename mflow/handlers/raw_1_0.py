import json


class Handler:

    @staticmethod
    def receive(receiver):
        header = receiver.next(as_json=True)
        return_value = None
        data = []

        while receiver.has_more():
            raw_data = receiver.next()
            if raw_data:
                data.append(raw_data)
            else:
                data.append(None)

        if header or data:
            return_value = {"header": header,
                            "data": data}

        return return_value


    @staticmethod
    def send(message, send, block=True):
        send(json.dumps(message["header"]).encode(), send_more=True, block=True)

        for data in message["data"]:
            send(data, block=block)



