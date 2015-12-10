
class Handler:

    header_details = 'all'  # values can be 'all', 'basic', 'none'

    def receive(self, receiver):
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