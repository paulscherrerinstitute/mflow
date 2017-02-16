import json


class Handler:

    header_details = 'all'  # values can be 'all', 'basic', 'none'

    @staticmethod
    def receive(receiver):
        header = receiver.next(as_json=True)
        return_value = dict()
        return_value['header'] = header

        header_detail = header['header_detail']

        # data = []

        # header contains: "htype", "shape", "type", "frame", "endianess", "source", "encoding", "tags"

        # Detector configuration
        if header_detail == 'all' or header_detail == 'basic':
            part_2 = receiver.next(as_json=True)
            return_value['part_2'] = part_2
        
        # Flatfield
        if header_detail == 'all':
            part_3 = receiver.next(as_json=True)
            return_value['part_3'] = part_3

        if header_detail == 'all':
            part_4_raw_data = receiver.next()
            return_value['part_4_raw'] = part_4_raw_data

        # Pixel Mask
        if header_detail == 'all':
            part_5 = receiver.next(as_json=True)
            return_value['part_5'] = part_5

        if header_detail == 'all':
            part_6_raw_data = receiver.next()
            return_value['part_6_raw'] = part_6_raw_data
        
        # Counterrate table
        if header_detail == 'all':
            part_7 = receiver.next(as_json=True)
            return_value['part_7'] = part_7

        if header_detail == 'all':
            part_8_raw_data = receiver.next()
            return_value['part_8_raw'] = part_8_raw_data

        if receiver.has_more():
            # In our cases appendix is always a JSON!
            appendix = receiver.next(as_json=True)
            return_value['appendix'] = appendix

        return return_value

    @staticmethod
    def send(message, send, block=True):
        detailed_header = message.data["header"]['header_detail'] == "all"
        has_appendix = "appendix" in message.data

        # Header and part_2 are always present.
        send(json.dumps(message.data["header"]).encode(), send_more=True, block=True)
        # Send more data if message has appendix or a detailed header.
        send(json.dumps(message.data["part_2"]).encode(), send_more=has_appendix or detailed_header, block=block)

        # Other parts only in complete header.
        if detailed_header:
            send(json.dumps(message.data["part_3"]).encode(), send_more=True, block=block)
            send(message.data["part_4_raw"], send_more=True, block=block)
            send(json.dumps(message["data"]["part_5"]).encode(), send_more=True, block=block)
            send(message.data["part_6_raw"], send_more=True, block=block)
            send(json.dumps(message.data["part_7"]).encode(), send_more=True, block=block)
            # Send more only if it has appendix.
            send(message.data["part_8_raw"], send_more=has_appendix, block=block)

        if has_appendix:
            send(json.dumps(message.data["appendix"]).encode(), send_more=False, block=block)
