"""
Test pubsub graph.
"""
import re
import time
import logging

from termcolor import cprint

import apache_beam as beam
import apache_beam.transforms.window as window
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
        self.words_counter = Metrics.counter(
            self.__class__, 'words')
        self.word_lengths_counter = Metrics.counter(
            self.__class__, 'word_lengths')
        self.word_lengths_dist = Metrics.distribution(
            self.__class__, 'word_len_dist')
        self.empty_line_counter = Metrics.counter(
            self.__class__, 'empty_lines')

    def process(self, element):
        """
        Returns an iterator over the words of this element.

        The element is a line of text.  If the line is blank, note that,
        too.
        """
        text_line = element.strip()
        if not text_line:
            self.empty_line_counter.inc(1)
        words = re.findall(r'[\w\']+', text_line, re.UNICODE)
        for w in words:
            self.words_counter.inc()
            self.word_lengths_counter.inc(len(w))
            self.word_lengths_dist.update(len(w))
        return words


def main(options):
    from rillbeam.helpers import pubsub_interface, pubsub_interface2
    from rillbeam.window import CustomWindow

    def count_ones(word_ones):
        (word, ones) = word_ones
        return word, sum(ones)

    def format_result(word_count):
        (word, count) = word_count
        return '%s: %d' % (word, count)

    pipe = beam.Pipeline(options=options)

    (
        pipe
        | 'PubSubInflow' >> beam.io.ReadFromPubSub(topic=INPUT_TOPIC)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | 'Split' >> (beam.ParDo(WordExtractingDoFn())
                      .with_output_types(unicode))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'Window' >> beam.WindowInto(CustomWindow(30))
        | 'GroupByKey' >> beam.GroupByKey()
        | 'CountOnes' >> beam.Map(count_ones)
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
        pubsub_interface2(SUBSCRIPTION_PATH, INPUT_TOPIC)
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
