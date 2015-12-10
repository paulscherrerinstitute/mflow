import zmq

class Handler:

    def receive(self, receiver):
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
