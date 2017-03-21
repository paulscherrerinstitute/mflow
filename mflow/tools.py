import time


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


class StreamStatisticsPrinter(object):
    """
    Record and display the user the stream message and bytes throughput.
    """

    # Bytes to mega bytes conversion factor.
    MB_FACTOR = 1 / (10 ** 6)

    def __init__(self, sampling_interval=0.2):
        """
        Initiate the stream statistics printer.
        :param sampling_interval: Minimum sampling interval.
        """
        self.sampling_interval = sampling_interval

        # Collect the initial time in case you need to print the summary.
        self.initial_time = time.time()

        # Keep track of the last statistics printed to the user.
        self.last_printed_statistics = {"total_bytes_received": 0,
                                        "messages_received": 0,
                                        "time": self.initial_time}

        # Keep track of the last statistics received.
        self.last_received_statistics = {"total_bytes_received": 0,
                                         "messages_received": 0,
                                         "time": self.initial_time}

    def process_statistics(self, message_statistics):
        """
        Should be called at every message received.
        :param message_statistics: Statistics of the received message.
        """
        current_time = time.time()

        # Save received statistics.
        self.last_received_statistics["total_bytes_received"] = message_statistics.total_bytes_received
        self.last_received_statistics["messages_received"] = message_statistics.messages_received
        self.last_received_statistics["time"] = current_time

        delta_time = current_time - self.last_printed_statistics["time"]
        # If the delta time is greater than the sampling rate, print the statistics.
        if delta_time > self.sampling_interval:
            self.print_statistics()

    def print_statistics(self):
        """
        Calculate the data rate and print the statistics to the standard output.
        """
        delta_time = self.last_received_statistics["time"] - self.last_printed_statistics["time"]

        # bytes/second in last interval.
        data_rate = (self.last_received_statistics["total_bytes_received"] -
                     self.last_printed_statistics["total_bytes_received"]) / delta_time

        # messages/second in last interval.
        message_rate = (self.last_received_statistics["messages_received"] -
                        self.last_printed_statistics["messages_received"]) / delta_time

        output = "Data rate: {data_rate: >10.3f} MB/s    Message rate: {message_rate: >10.3f} Hz" \
            .format(data_rate=data_rate * self.MB_FACTOR, message_rate=message_rate)

        print(output)

        # Update last printed statistics.
        self.last_printed_statistics.update(self.last_received_statistics)

    def print_summary(self):
        """
        Print statistics summary to standard output.
        """
        # Total elapsed time.
        delta_time = self.last_received_statistics["time"] - self.initial_time
        # Print summary if any messages were received.
        if delta_time > 0:
            print("_" * 60)

            data_rate = self.last_received_statistics["total_bytes_received"] / delta_time
            message_rate = self.last_received_statistics["messages_received"] / delta_time

            print("Total elapsed time:   {: >10.3f} s".format(delta_time))
            print("Average message size: {: >10.3f} MB".format(
                self.last_received_statistics["total_bytes_received"] /
                self.last_received_statistics["messages_received"] * self.MB_FACTOR))
            print("Total bytes received: {: >10.3f} MB".format(
                self.last_received_statistics["total_bytes_received"] * self.MB_FACTOR))
            print("Average data rate:    {: >10.3f} MB/s".format(data_rate * self.MB_FACTOR))
            print("Messages received:    {: >10d} messages".format(self.last_received_statistics["messages_received"]))
            print("Average message rate: {: >10.3f} Hz".format(message_rate))
        # No messages were received.
        else:
            print("No messages received.")

        print("_" * 60)

    def close(self, print_summary=True):
        """
        You need to close the statistics in order to flush any non printed statistics.
        :param print_summary: True if you want the summary to be printed.
        """
        # Note: The sampling interval, when flushing, is not honored.
        delta_time = self.last_received_statistics["time"] - self.last_printed_statistics["time"]
        # If the last received is not the same as the last printed statistics, print it.
        if delta_time > 0:
            self.print_statistics()

        if print_summary:
            self.print_summary()
