class Handler:

    def receive(self, receiver):

        header = receiver.next(as_json=True)
        return_value = None
        data = []

        # Receiving data
        while receiver.has_more():
            raw_data = receiver.next()
            if raw_data:
                data.append(raw_data)
            else:
                data.append(None)

        if header or data:
            return_value = {'header': header,
                            'data': data}

        return return_value
