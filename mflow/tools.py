class RoundRobinStrategy:

    def __init__(self):
        self.last_stream_read = None

    def next(self, streams):
        if self.last_stream_read is None:
            self.last_stream_read = 0
            return 0
        else:
            self.last_stream_read += 1
            self.last_stream_read %= len(streams)
            return self.last_stream_read


class Merge:
    """
    Utility class to merge multiple streams to behave as one.
    """

    def __init__(self, *arg, receive_strategy=RoundRobinStrategy()):
        self.streams = arg
        self.receive_strategy = receive_strategy

    def receive(self, handler=None, block=True):
        message = None
        count = 0
        #
        while (message is None or message.data is None) and count < len(self.streams):
            index_stream = self.receive_strategy.next(self.streams)
            print(index_stream)
            message = self.streams[index_stream].receive(handler=handler, block=False)
            count += 1

        # TODO need to update statistics
        # TODO need to decide what to do if block = True

        return message

    def disconnect(self):
        """
        As this class should somehow behave as a stream this function will close all involved streams
        """
        for stream in self.streams:
            stream.disconnect()

