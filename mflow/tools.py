import time
from argparse import Namespace
from collections import OrderedDict
from collections import deque
from logging import getLogger


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


class ThroughputStatistics(object):
    """
    Utility to calculate the stream throughput based on the mflow statistics.
    """
    # Bytes to mega bytes conversion factor.
    MB_FACTOR = 1 / (10 ** 6)

    def __init__(self, buffer=None, namespace=None, sampling_interval=0.2):
        """
        Initialize the statistics class.
        :param buffer: Circular buffer for saving statistics events. Default: None.
        :type buffer: collections.deque
        :param namespace: Namespace to use temporary variables. Default: None.
        :type namespace: argparse.Namespace
        :param sampling_interval: Sampling interval for adding new statistic events.
        """
        self.sampling_interval = sampling_interval
        self._logger = getLogger(self.__class__.__name__)

        # Use provided buffer or create a new one.
        if buffer is None:
            self._buffer = deque(maxlen=100)
        else:
            self._buffer = buffer

        # Use provided namespace or create a new one.
        if namespace is None:
            self.n = Namespace()
        else:
            self.n = namespace

        # Collect the initial time in case you need to print the summary.
        self.n.initial_time = time.time()

        # Keep track of the last statistics printed to the user.
        self.n.last_sampled_statistics = {"total_bytes_received": 0,
                                          "messages_received": 0,
                                          "time": self.n.initial_time}

        # Keep track of the last statistics received.
        self.n.last_received_statistics = {"total_bytes_received": 0,
                                           "messages_received": 0,
                                           "time": self.n.initial_time}

    def save_statistics(self, message_statistics):
        """
        Save new message statistics to the buffer.
        :param message_statistics: Statistics to process.
        :return: True if sampling interval was reached, False otherwise.
        """
        current_time = time.time()

        # Save received statistics.
        self.n.last_received_statistics["total_bytes_received"] = message_statistics.total_bytes_received
        self.n.last_received_statistics["messages_received"] = message_statistics.messages_received
        self.n.last_received_statistics["time"] = current_time

        delta_time = current_time - self.n.last_sampled_statistics["time"]
        # If the delta time is greater than the sampling rate, print the statistics.
        if delta_time > self.sampling_interval:
            self._save_statistics_to_buffer()
            return True
        # The sampling interval was not reached, no new statistics events.
        return False

    def _save_statistics_to_buffer(self):
        """
        Calculate the data rate and print the statistics to the standard output.
        """
        delta_time = self.n.last_received_statistics["time"] - self.n.last_sampled_statistics["time"]

        # bytes/second in last interval.
        data_rate = (self.n.last_received_statistics["total_bytes_received"] -
                     self.n.last_sampled_statistics["total_bytes_received"]) / delta_time

        # messages/second in last interval.
        message_rate = (self.n.last_received_statistics["messages_received"] -
                        self.n.last_sampled_statistics["messages_received"]) / delta_time

        self._buffer.append({"message_rate": message_rate,
                             "data_rate": data_rate})

        # Update last printed statistics.
        self.n.last_sampled_statistics.update(self.n.last_received_statistics)

        # Append statistics to logger.
        self._logger.info("Data rate: {data_rate: >10.3f} MB/s    Message rate: {message_rate: >10.3f} Hz"
                          .format(data_rate=data_rate * self.MB_FACTOR, message_rate=message_rate))

    def get_last_sampled_statistics(self):
        """
        Print the latest sampled statistics.
        :return: Dict with latest statistics.
        """
        try:
            return self._buffer[-1]
        except IndexError:
            return None

    def get_statistics(self):
        """
        Get aggregated statistics.
        :return: Dict with summary or {} if no statistics is available.
        """
        delta_time = self.n.last_received_statistics["time"] - self.n.initial_time
        # Get statistics if any message was received.
        if delta_time > 0:
            average_message_size = self.n.last_received_statistics["total_bytes_received"] / \
                                   self.n.last_received_statistics["messages_received"]

            statistics = {"total_elapsed_time": delta_time,
                          "average_message_size": average_message_size,
                          "total_bytes_received": self.n.last_received_statistics["total_bytes_received"],
                          "average_data_rate": self.n.last_received_statistics["total_bytes_received"] / delta_time,
                          "messages_received": self.n.last_received_statistics["messages_received"],
                          "average_message_rate": self.n.last_received_statistics["messages_received"] / delta_time}

            return OrderedDict(sorted(statistics.items()))

        # No messages were received.
        return {}

    def get_statistics_raw(self):
        """
        Return the raw statistics data.
        :return: List of statistic events.
        """
        return self._buffer

    def flush(self):
        """
        If there are statistic messages that were not processed yet (because the sampling interval was not reached)
        this events will be added to the statistics (but the sampling interval will not be honored in this case).
        :return: True if new statistic events were added, False otherwise.
        """
        delta_time = self.n.last_received_statistics["time"] - self.n.last_sampled_statistics["time"]
        # If the last received is not the same as the last printed statistics, process it.
        # Note: The sampling interval, when flushing, is not honored.
        if delta_time > 0:
            self._save_statistics_to_buffer()
            return True
        # Nothing was added to the statistics events.
        return False


class ThroughputStatisticsPrinter(object):
    """
    Wrapper to save and display the stream statistics.
    """

    def __init__(self, sampling_interval=0.2):
        """
        Initiate the stream statistics printer.
        :param sampling_interval: Minimum sampling interval.
        """
        self.statistics = ThroughputStatistics(sampling_interval=sampling_interval)

    def save_statistics(self, message_statistics):
        """
        Should be called at every message received.
        :param message_statistics: Statistics of the received message.
        """
        if self.statistics.save_statistics(message_statistics):
            self.print_statistics()

    def print_statistics(self):
        """
        Print the data and message rate to console.
        """
        latest_statistics = self.statistics.get_last_sampled_statistics()

        # bytes/second in last interval.
        data_rate = latest_statistics["data_rate"]
        # messages/second in last interval.
        message_rate = latest_statistics["message_rate"]

        output = "Data rate: {data_rate: >10.3f} MB/s    Message rate: {message_rate: >10.3f} Hz" \
            .format(data_rate=data_rate * self.statistics.MB_FACTOR, message_rate=message_rate)

        print(output)

    def print_summary(self):
        """
        Print statistics summary to standard output.
        """
        statistics_summary = self.statistics.get_statistics()
        if statistics_summary:
            print("_" * 60)

            print("Total elapsed time:   {: >10.3f} s".format(statistics_summary["total_elapsed_time"]))
            print("Average message size: {: >10.3f} MB".format(
                statistics_summary["average_message_size"] * self.statistics.MB_FACTOR))
            print("Total bytes received: {: >10.3f} MB".format(
                statistics_summary["total_bytes_received"] * self.statistics.MB_FACTOR))
            print("Average data rate:    {: >10.3f} MB/s".format(
                statistics_summary["average_data_rate"] * self.statistics.MB_FACTOR))
            print("Messages received:    {: >10d} messages".format(statistics_summary["messages_received"]))
            print("Average message rate: {: >10.3f} Hz".format(statistics_summary["average_message_rate"]))
        # No messages were received.
        else:
            print("No messages received.")

        print("_" * 60)

    def close(self, print_summary=True):
        """
        You need to close the statistics in order to flush any non printed statistics.
        :param print_summary: True if you want the summary to be printed.
        """
        if self.statistics.flush():
            self.print_statistics()

        if print_summary:
            self.print_summary()
