import json


class Handler:

    header_details = "all"  # values can be 'all', 'basic', 'none'

    @staticmethod
    def receive(receiver):
        # header contains: "htype", "shape", "type", "frame", "endianness", "source", "encoding", "tags"
        header = receiver.next(as_json=True)

        res = {"header": header}

        header_detail = header["header_detail"]

        if header_detail == "all" or header_detail == "basic":
            # Detector configuration
            part_2 = receiver.next(as_json=True)
            res["part_2"] = part_2

        if header_detail == "all":
            # Flatfield
            part_3 = receiver.next(as_json=True)
            part_4_raw = receiver.next()
            res["part_3"] = part_3
            res["part_4_raw"] = part_4_raw

            # Pixel Mask
            part_5 = receiver.next(as_json=True)
            part_6_raw = receiver.next()
            res["part_5"] = part_5
            res["part_6_raw"] = part_6_raw

            # Counterrate table
            part_7 = receiver.next(as_json=True)
            part_8_raw = receiver.next()
            res["part_7"] = part_7
            res["part_8_raw"] = part_8_raw

        if receiver.has_more():
            # appendix is always json
            appendix = receiver.next(as_json=True)
            res["appendix"] = appendix

        return res


    @staticmethod
    def send(message, send, block=True):
        detailed_header = message["header"]["header_detail"] == "all"
        has_appendix = "appendix" in message

        # Header and part_2 are always present.
        send(json.dumps(message["header"]).encode(), send_more=True, block=True)
        # Send more data if message has appendix or a detailed header.
        send(json.dumps(message["part_2"]).encode(), send_more=has_appendix or detailed_header, block=block)

        # Other parts only in complete header.
        if detailed_header:
            send(json.dumps(message["part_3"]).encode(), send_more=True, block=block)
            send(message["part_4_raw"], send_more=True, block=block)
            send(json.dumps(message["part_5"]).encode(), send_more=True, block=block)
            send(message["part_6_raw"], send_more=True, block=block)
            send(json.dumps(message["part_7"]).encode(), send_more=True, block=block)
            # Send more only if it has appendix.
            send(message["part_8_raw"], send_more=has_appendix, block=block)

        if has_appendix:
            send(json.dumps(message["appendix"]).encode(), send_more=False, block=block)



