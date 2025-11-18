import argparse

import mflow
from mflow.tools import ThroughputStatisticsPrinter


def main():
    parser = argparse.ArgumentParser(description="Stream statistic utility")
    parser.add_argument("source", type=str, help='Source address - format "tcp://<address>:<port>"')
    parser.add_argument("-m", "--mode", default="pull", choices=["pull", "sub"], type=str,
                        help="Communication mode - either pull (default) or sub")
    parser.add_argument("-i", "--sampling_interval", type=float, default=0.5,
                        help="Interval in seconds at which to sample the stream.\n"
                             "If zero, every packet will be sampled.")
    arguments = parser.parse_args()

    address = arguments.source
    mode = mflow.SUB if arguments.mode == "sub" else mflow.PULL
    stream = mflow.connect(address, mode=mode, receive_timeout=1000)
    statistics_printer = ThroughputStatisticsPrinter(sampling_interval=arguments.sampling_interval)

    def dump(receiver):
        """
        Just read the stream.
        :param receiver: Function to use as a receiver.
        :return: 1 if the reception was successful, None if it timed out.
        """
        data = receiver.next()
        while receiver.has_more():
            # If any of the message parts time outed (the only way a message part can be None)
            if receiver.next() is None:
                return None
        # Return 1 only for valid data.
        return 1 if data else None

    print("mflow stats started. Sampling interval is %.2f seconds." % arguments.sampling_interval)
    print("_" * 60)

    try:
        while True:
            message = stream.receive(handler=dump)
            if message is not None:
                statistics_printer.save_statistics(message.statistics)
    except KeyboardInterrupt:
        stream.disconnect()
        # Flush and Print summary.
        statistics_printer.close()





if __name__ == "__main__":
    main()



