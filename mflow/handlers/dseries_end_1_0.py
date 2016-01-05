
class Handler:

    def receive(self, receiver):

        header = receiver.next(as_json=True)
        return {'header': header}
