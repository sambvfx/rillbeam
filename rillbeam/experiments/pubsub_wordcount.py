"""
Test pubsub graph.
"""
import time
import logging

from termcolor import cprint

import apache_beam as beam
from apache_beam.runners.runner import PipelineState
from apache_beam.metrics import Metrics

from apache_beam.runners.runner import PipelineResult


# Google pubsub topics and subscriptions
INPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-inflow'
OUTPUT_TOPIC = 'projects/dataflow-241218/topics/rillbeam-outflow'
SUBSCRIPTION_PATH = 'projects/dataflow-241218/subscriptions/manual'


class WordExtractingDoFn(beam.DoFn):
    """
    Parse each line of input text into words.
    """

    def __init__(self):
        # self.words_counter = Metrics.counter(
        #     self.__class__, 'words')
        # self.word_lengths_counter = Metrics.counter(
        #     self.__class__, 'word_lengths')
        # self.word_lengths_dist = Metrics.distribution(
        #     self.__class__, 'word_len_dist')
        self.empty_line_counter = Metrics.counter(
            self.__class__, 'empty_lines')

    def process(self, element):
        """
        yield a tuple of key and a bool indicating whether more of this
        key remain
        """
        text_line = element.strip()
        if not text_line:
            self.empty_line_counter.inc(1)
        parts = text_line.split()
        if len(parts) == 1:
            yield parts[0], True
        else:
            yield parts[0], False


def main(options):
    from rillbeam.helpers import pubsub_interface, pubsub_interface2
    from rillbeam.window import CustomWindow

    def is_final(x):
        return isinstance(x, tuple) and len(x) == 2 and not x[1]

    def format_result(pair):
        return '%s: %s' % pair

    pipe = beam.Pipeline(options=options)

    (
        pipe
        | 'PubSubInflow' >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | 'Split' >> beam.ParDo(WordExtractingDoFn())
        | 'Window' >> beam.WindowInto(CustomWindow(30, is_final))
        | 'GroupByKey' >> beam.GroupByKey()
        | 'Format' >> beam.Map(format_result)
        | 'ToBytes' >> beam.Map(lambda x: bytes(x))
        | 'PubSubOutflow' >> beam.io.WriteToPubSub(OUTPUT_TOPIC)
    )

    print
    cprint('Starting pipeline...', 'yellow', attrs=['bold'])
    result = pipe.run()  # type: PipelineResult
    time.sleep(10)
    while result.state != PipelineState.RUNNING:
        time.sleep(10)

    try:
        pubsub_interface(SUBSCRIPTION_PATH, INPUT_TOPIC)
    finally:
        print
        cprint('Shutting down pipeline...', 'yellow', attrs=['bold'])
        result.cancel()
        print


if __name__ == '__main__':
    from rillbeam.helpers import get_options
    pipeline_args, _ = get_options(
        __name__, None,
        'streaming',
    )
    logging.getLogger().setLevel(logging.INFO)
    main(pipeline_args)
