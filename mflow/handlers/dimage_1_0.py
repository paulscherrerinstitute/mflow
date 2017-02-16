import json

class Handler:

    @staticmethod
    def receive(receiver):
        header = receiver.next(as_json=True)
        return_value = {}

        part_2 = receiver.next(as_json=True)
        part_3_raw_data = receiver.next()

        part_4 = receiver.next(as_json=True)

        if receiver.has_more():
            # In our cases appendix is always a JSON!
            appendix = receiver.next(as_json=True)
            return_value['appendix'] = appendix

        return_value['header'] = header
        return_value['part_2'] = part_2
        return_value['part_3_raw'] = part_3_raw_data  # Image data
        return_value['part_4'] = part_4
        
        return return_value

    @staticmethod
    def send(message, send, block=True):
        send(json.dumps(message.data["header"]).encode(), send_more=True, block=True)
        send(json.dumps(message.data["part_2"]).encode(), send_more=True, block=block)
        send(message.data["part_3_raw"], send_more=True, block=block)
        send(json.dumps(message.data["part_4"]).encode(), send_more=False, block=block)
