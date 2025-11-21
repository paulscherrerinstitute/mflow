import json


class Handler:

    @staticmethod
    def receive(receiver):
        header = receiver.next(as_json=True)
        part_2 = receiver.next(as_json=True)
        part_3_raw = receiver.next()
        part_4 = receiver.next(as_json=True)

        res = {
            "header": header,
            "part_2": part_2,
            "part_3_raw": part_3_raw,
            "part_4": part_4
        }

        if receiver.has_more():
            # appendix is always a json
            appendix = receiver.next(as_json=True)
            res["appendix"] = appendix

        return res


    @staticmethod
    def send(message, send, block=True):
        send(json.dumps(message["header"]).encode(), send_more=True, block=True)
        send(json.dumps(message["part_2"]).encode(), send_more=True, block=block)
        send(message["part_3_raw"], send_more=True, block=block)
        send(json.dumps(message["part_4"]).encode(), send_more=False, block=block)
        #TODO: should this optionally send the appendix?



